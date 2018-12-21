/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.output.statement.operations.snowflake;

import lombok.Data;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Hex;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.operations.QueryManager;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.nio.file.Files.*;
import static java.time.LocalDateTime.now;
import static java.util.Collections.emptyMap;
import static java.util.Locale.ROOT;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Slf4j
public class SnowflakeCopy extends QueryManager {

    private final long maxChunk = 16 * 1024 * 1024; // 16MB

    private final Path snowflakeTmpDir;

    private final int putParallelism;

    public SnowflakeCopy(final Platform platform, final OutputConfig configuration, final I18nMessage i18n,
            final JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
        this.putParallelism = Math.min(99, 4 * Runtime.getRuntime().availableProcessors());
        try {
            snowflakeTmpDir = createTempDirectory("talend-jdbc-snowflake-");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (snowflakeTmpDir != null && snowflakeTmpDir.toFile().exists()) {
                        Files.walk(snowflakeTmpDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                    }
                } catch (final IOException e) {
                    log.error("can't clean tmp files for snowflake put", e);
                }
            }));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<Reject> execute(final List<Record> records) throws SQLException {
        final List<Reject> rejects = new ArrayList<>();
        final List<RecordChunk> chunks = splitRecords(snowflakeTmpDir, records);
        try (final Connection connection = getDataSource().getConnection()) {
            final String qSchema = getPlatform().identifier(connection.getSchema());
            final String qDatabase = getPlatform().identifier(connection.getCatalog());
            final String tableName = getConfiguration().getDataset().getTableName();
            final String fqTableName = qDatabase + "." + qSchema + "." + getPlatform().identifier(tableName);
            final String fqStageName = qDatabase + "." + qSchema + ".%" + getPlatform().identifier(tableName);
            try (final Statement statement = connection.createStatement()) {
                final List<RecordChunk> chunksToCopy = chunks.stream().parallel()
                        .map(chunk -> doPUT(fqStageName, statement, chunk, rejects)).filter(Objects::nonNull).collect(toList());

                final List<CopyError> copyErrors = doCopy(fqStageName, fqTableName, statement, chunksToCopy);
                if (!copyErrors.isEmpty()) {
                    if (copyErrors.size() == chunks.size()) {
                        throw new IllegalStateException(copyErrors.stream().map(CopyError::getError).collect(joining("\n")));
                    }
                    rejects.addAll(validationToReject(chunks, copyErrors));
                }
            }
            connection.commit();
        }

        return rejects;
    }

    private List<Reject> validationToReject(final List<RecordChunk> chunks, final List<CopyError> validationResult) {
        return validationResult.stream()
                .flatMap(e -> chunks.stream().filter(chunk -> chunk.getChunk().getFileName().toString().equals(e.getFile()))
                        .flatMap(chunk -> chunk.getRecords().stream().map(record -> new Reject(e.getError(), record))))
                .collect(toList());
    }

    private RecordChunk doPUT(final String fqStageName, final Statement statement, final RecordChunk chunk,
            final List<Reject> rejects) {
        try (final ResultSet result = statement.executeQuery(
                "PUT '" + chunk.getChunk().toUri() + "' '@" + fqStageName + "/' AUTO_COMPRESS=TRUE PARALLEL=" + putParallelism)) {
            if (!"UPLOADED".equalsIgnoreCase(result.getString("status"))) {
                String error = result.getString("message");
                rejects.addAll(toReject(chunk, error, result.getString("status"), null));
                return null;
            }
        } catch (final SQLException e) {
            rejects.addAll(toReject(chunk, e.getMessage(), e.getSQLState(), e.getErrorCode()));
            return null;
        }

        return chunk;
    }

    private List<Reject> toReject(RecordChunk chunk, String error, final String state, final Integer code) {
        return chunk.getRecords().stream().map(record -> new Reject(error, state, code, record)).collect(Collectors.toList());
    }

    private List<CopyError> doCopy(final String fqStageName, final String fqTableName, final Statement statement,
            final List<RecordChunk> chunks) {
        final List<CopyError> errors = new ArrayList<>();
        try (final ResultSet result = statement
                .executeQuery("COPY INTO " + fqTableName + " from '@" + fqStageName + "/'" + " FILES="
                        + chunks.stream().map(chunk -> chunk.getChunk().getFileName()).map(name -> "'" + name + ".gz'")
                                .collect(joining(",", "(", ")"))
                        + " FILE_FORMAT=(TYPE=CSV field_delimiter=',' COMPRESSION=GZIP field_optionally_enclosed_by='\"')"
                        + "PURGE=TRUE ON_ERROR='CONTINUE'")) {
            while (result.next()) {
                final String status = result.getString("status");
                switch (status.toLowerCase(ROOT)) {
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
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        return errors;
    }

    @Data
    private static class CopyError {

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
        records.stream().map(
                record -> record.getSchema().getEntries().stream().map(entry -> valueOf(record, entry)).collect(joining(",")))
                .forEach(line -> {
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
    public static class RecordChunk {

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
                final String suffix = now(ZoneOffset.UTC).format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                try {
                    chunk = createTempFile(tmpDir, "part_" + part + "_", "_" + suffix + ".csv");
                    writer = newBufferedWriter(chunk, StandardCharsets.UTF_8);
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

    @Override
    protected String buildQuery(final List<Record> records) {
        return null;
    }

    @Override
    protected Map<Integer, Schema.Entry> getQueryParams() {
        return emptyMap();
    }

    @Override
    protected boolean validateQueryParam(final Record record) {
        return true;
    }

    private static String valueOf(final Record record, final Schema.Entry entry) {
        switch (entry.getType()) {
        case INT:
            return String.valueOf(record.getInt(entry.getName()));
        case LONG:
            return String.valueOf(record.getLong(entry.getName()));
        case BYTES:
            return Hex.encodeHexString(record.getBytes(entry.getName()));
        case FLOAT:
            return Float.toHexString(record.getFloat(entry.getName()));
        case DOUBLE:
            return Double.toHexString(record.getDouble(entry.getName()));
        case BOOLEAN:
            return String.valueOf(record.getBoolean(entry.getName()));
        case DATETIME:
            return String.valueOf(record.getDateTime(entry.getName()).toInstant().toEpochMilli());
        case STRING:
            return escape(record.getString(entry.getName()));
        case ARRAY:
        case RECORD:
        default:
            throw new IllegalArgumentException("unsupported type in " + entry);
        }
    }

    private static String escape(final String value) {
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
}
