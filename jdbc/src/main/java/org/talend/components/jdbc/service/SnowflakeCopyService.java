/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.service;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.apache.commons.codec.binary.Hex;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.statement.operations.QueryManagerImpl;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SnowflakeCopyService implements Serializable {

    private static final long maxChunk = 16 * 1024 * 1024; // 16MB

    private static final String TIMESTAMP_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    private final List<Path> tmpFiles = new ArrayList<>();

    private Path tmpFolder;

    public List<Reject> putAndCopy(final Connection connection, final List<Record> records, final String fqStageName,
            final String fqTableName, final String fqTmpTableName) throws SQLException {
        try (final Statement statement = connection.createStatement()) {
            statement.execute("create temporary table if not exists " + fqTmpTableName + " like " + fqTableName);
        }
        return putAndCopy(connection, records, fqStageName, fqTmpTableName);
    }

    public List<Reject> putAndCopy(final Connection connection, final List<Record> records, final String fqStageName,
            final String fqTableName) {
        final List<RecordChunk> chunks = splitRecords(createWorkDir(), records);
        final List<Reject> rejects = new ArrayList<>();
        final List<RecordChunk> copy = chunks.stream().parallel().map(chunk -> doPUT(fqStageName, connection, chunk, rejects))
                .filter(Objects::nonNull).collect(Collectors.toList());
        rejects.addAll(toReject(chunks, doCopy(fqStageName, fqTableName, connection, copy)));
        return rejects;
    }

    /**
     * @param tableName the original table name
     * @return a tmp table name from the original table name in format tmp_tableName_yyyyMMddHHmmss
     */

    public String tmpTableName(final String tableName) {
        final String suffix = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
        String tmpTableName = "tmp_" + tableName + "_" + suffix;
        return tmpTableName.length() < 256 ? tmpTableName : tmpTableName.substring(0, 255);
    }

    private Path createWorkDir() {
        try {
            tmpFolder = Files.createTempDirectory("talend-jdbc-snowflake-");
            log.debug("Temp folder {} created.", tmpFolder);
            return tmpFolder;
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private List<Reject> toReject(final List<RecordChunk> chunks, final List<CopyError> errors) {
        return errors.stream().flatMap(error -> chunks.stream()
                .filter(chunk -> error.getFile().startsWith(chunk.getChunk().getFileName().toString()))
                .map(chunk -> new Reject(
                        error.getError() + (error.getErrorColumnName() == null || error.getErrorColumnName().isEmpty() ? ""
                                : ", columnName=" + error.getErrorColumnName()),
                        chunk.getRecords().get(error.getErrorLine() - 1))))
                .collect(Collectors.toList());
    }

    private RecordChunk doPUT(final String fqStageName, final Connection connection, final RecordChunk chunk,
            final List<Reject> rejects) {
        try (final Statement statement = connection.createStatement()) {
            try (final ResultSet result = statement
                    .executeQuery("PUT '" + chunk.getChunk().toUri() + "' '@" + fqStageName + "/' AUTO_COMPRESS=TRUE")) {
                result.next();
                if (!"uploaded".equalsIgnoreCase(result.getString("status"))) {
                    String error = result.getString("message");
                    rejects.addAll(toReject(chunk, error, result.getString("status"), null));
                    return null;
                }
            }
            return chunk;
        } catch (final SQLException e) {
            rejects.addAll(toReject(chunk, e.getMessage(), e.getSQLState(), e.getErrorCode()));
            return null;
        }
    }

    private List<Reject> toReject(RecordChunk chunk, String error, final String state, final Integer code) {
        return chunk.getRecords().stream().map(record -> new Reject(error, state, code, record)).collect(Collectors.toList());
    }

    private List<CopyError> doCopy(final String fqStageName, final String fqTableName, final Connection connection,
            final List<RecordChunk> chunks) {
        final List<CopyError> errors = new ArrayList<>();
        try (final Statement statement = connection.createStatement()) {
            try (final ResultSet result = statement
                    .executeQuery("COPY INTO " + fqTableName + " from '@" + fqStageName + "'" + " FILES="
                            + chunks.stream().map(chunk -> chunk.getChunk().getFileName()).map(name -> "'" + name + ".gz'")
                                    .collect(Collectors.joining(",", "(", ")"))
                            + " FILE_FORMAT=(TYPE=CSV field_delimiter=',' COMPRESSION=GZIP field_optionally_enclosed_by='\"')"
                            + " PURGE=TRUE ON_ERROR='CONTINUE'")) {
                while (result.next()) {
                    final String status = result.getString("status");
                    switch (status.toLowerCase(Locale.ROOT)) {
                    case "load_failed":
                    case "partially_loaded":
                        final String file = result.getString("file");
                        final String error = result.getString("first_error");
                        final int errorLine = result.getInt("first_error_line");
                        final int errorsSeen = result.getInt("errors_seen");
                        final int errorLimit = result.getInt("error_limit");
                        final int errorCharacter = result.getInt("first_error_character");
                        final String errorColumnName = result.getString("first_error_column_name");
                        final int rowsLoaded = result.getInt("rows_loaded");
                        final int rowsParsed = result.getInt("rows_parsed");
                        errors.add(new CopyError(file, errorsSeen, errorLimit, error, errorLine, errorCharacter, errorColumnName,
                                rowsLoaded, rowsParsed));
                        break;
                    case "loaded":
                        break;
                    }
                }
            }
            return errors;
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    @Data
    private class CopyError {

        private final String file;

        private final int errorSeen;

        private final int errorLimit;

        private final String error;

        private final int errorLine;

        private final int errorCharacter;

        private final String errorColumnName;

        private final int rowLoaded;

        private final int rowParsed;
    }

    private List<RecordChunk> splitRecords(final Path directoryPath, final List<Record> records) {
        final AtomicLong size = new AtomicLong(0);
        final AtomicInteger count = new AtomicInteger(0);
        final AtomicInteger recordCounter = new AtomicInteger(0);
        final Map<Integer, RecordChunk> chunks = new HashMap<>();
        records.stream().map(record -> record.getSchema().getEntries().stream().map(entry -> format(record, entry))
                .collect(Collectors.joining(","))).forEach(line -> {
                    if (size.addAndGet(line.getBytes(StandardCharsets.UTF_8).length) > maxChunk) {
                        // this writer can be closed now. to early free of memory
                        chunks.get(count.getAndIncrement()).close();
                        size.set(line.getBytes(StandardCharsets.UTF_8).length);
                    }
                    final int recordNumber = recordCounter.getAndIncrement();
                    chunks.computeIfAbsent(count.get(), key -> new RecordChunk(records, key, recordNumber, directoryPath))
                            .writer(line);
                });
        chunks.get(count.get()).close(); // close the last writer
        return new ArrayList<>(chunks.values());
    }

    @Getter
    @RequiredArgsConstructor
    private class RecordChunk {

        private final List<Record> records;

        private final int part;

        private final int start;

        private final Path tmpDir;

        private Path chunk;

        private BufferedWriter writer;

        private int end;

        List<Record> getRecords() {
            if (records == null) {
                return null;
            }
            return records.subList(start, end);
        }

        void writer(final String line) {
            if (writer == null) {
                end = start;
                final String suffix = LocalDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                try {
                    chunk = Files.createTempFile(tmpDir, "part_" + part + "_", "_" + suffix + ".csv");
                    log.debug("Temp file {} created", chunk);
                    tmpFiles.add(chunk);
                    writer = Files.newBufferedWriter(chunk, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
            try {
                writer.write(line);
                writer.newLine();
                end++;
            } catch (final IOException e) {
                throw new IllegalStateException(e);
            }
        }

        void close() {
            if (writer != null) {
                try {
                    writer.close();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
    }

    private String format(final Record record, final Schema.Entry entry) {
        switch (entry.getType()) {
        case INT:
        case LONG:
        case BOOLEAN:
            return QueryManagerImpl.valueOf(record, entry).map(String::valueOf).orElse("");
        case FLOAT:
            return QueryManagerImpl.valueOf(record, entry).map(v -> Float.toHexString((Float) v)).orElse("");
        case DOUBLE:
            return QueryManagerImpl.valueOf(record, entry).map(v -> Double.toHexString((Double) v)).orElse("");
        case BYTES:
            return QueryManagerImpl.valueOf(record, entry).map(v -> Hex.encodeHexString((byte[]) v)).orElse("");
        case DATETIME:
            return QueryManagerImpl.valueOf(record, entry)
                    .map(v -> ((ZonedDateTime) v).format(DateTimeFormatter.ofPattern(TIMESTAMP_FORMAT_PATTERN))).orElse("");
        case STRING:
            return escape(record.getString(entry.getName()));
        case ARRAY:
        case RECORD:
        default:
            throw new IllegalArgumentException(
                    "Unsupported \"" + entry.getType().name() + "\" type for field: " + entry.getName());
        }
    }

    private String escape(final String value) {
        if (value == null) {
            return "";
        }
        if (value.isEmpty()) {
            return "\"\"";
        }
        if (value.indexOf('"') >= 0 || value.indexOf('\n') >= 0 || value.indexOf(',') >= 0 || value.indexOf('\\') >= 0) {
            return '"' + value.replaceAll("\"", "\"\"") + '"';
        } else {
            return value;
        }
    }

    public void cleanTmpFiles() {
        Consumer<Path> deletePath = p -> {
            try {
                log.debug("Deleting temp file/forlder: {}", p);
                Files.deleteIfExists(p);
            } catch (IOException e) {
                log.warn("Cannot clean tmp file/forlder '{}'", p);
            }
        };
        tmpFiles.stream().forEach(deletePath);
        deletePath.accept(tmpFolder);
    }
}
