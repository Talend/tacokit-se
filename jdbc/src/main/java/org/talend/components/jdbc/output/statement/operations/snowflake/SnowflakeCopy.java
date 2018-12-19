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
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

@Slf4j
public class SnowflakeCopy extends QueryManager {

    private final long maxChunk = 64 * 1024 * 1024; // 64 MB

    private final Path snowflakeTempDir;

    public SnowflakeCopy(final Platform platform, final OutputConfig configuration, final I18nMessage i18n,
            final JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
        try {
            snowflakeTempDir = createTempDirectory("talend-jdbc-snowflake-");
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    if (snowflakeTempDir != null && snowflakeTempDir.toFile().exists()) {
                        Files.walk(snowflakeTempDir).sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
                    }
                } catch (final IOException e) {
                    log.warn("can't clean temp chunk", e);
                }
            }));
        } catch (final IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public List<Reject> execute(final List<Record> records) throws SQLException {
        final List<Reject> rejects = new ArrayList<>();
        final Collection<RecordChunk> chunks = splitRecords(snowflakeTempDir, records);
        try (final Connection connection = getDataSource().getConnection()) {
            final String qSchema = getPlatform().identifier(connection.getSchema());
            final String qDatabase = getPlatform().identifier(connection.getCatalog());
            final String fqStageName = qDatabase + "." + qSchema + ".%"
                    + getPlatform().identifier(getConfiguration().getDataset().getTableName());
            final String fqTableName = qDatabase + "." + qSchema + "."
                    + getPlatform().identifier(getConfiguration().getDataset().getTableName());
            try (final Statement statement = connection.createStatement()) {
                final List<RecordChunk> chunkToCopy = chunks.parallelStream()
                        .map(chunk -> doPUT(fqStageName, statement, chunk, rejects)).filter(Objects::nonNull).collect(toList());

                rejects.addAll(doCopy(fqStageName, fqTableName, statement, chunkToCopy));

            }
            connection.commit();
        }

        return rejects;
    }

    private RecordChunk doPUT(final String fqStageName, final Statement statement, final RecordChunk chunk,
            final List<Reject> rejects) {
        try {
            statement.execute("PUT '" + chunk.getChunk().toUri() + "' '@" + fqStageName + "/' AUTO_COMPRESS=TRUE");
            return chunk;
        } catch (final SQLException e) {
            rejects.addAll(chunk.getRecords().stream()
                    .map(record -> new Reject(e.getMessage(), e.getSQLState(), e.getErrorCode(), record))
                    .collect(Collectors.toList()));
            return null;
        }
    }

    private List<Reject> doCopy(final String fqStageName, final String fqTableName, final Statement statement,
            final List<RecordChunk> chunks) {
        final List<CopyResult> errors = new ArrayList<>();
        try (final ResultSet result = statement
                .executeQuery("COPY INTO " + fqTableName + " from '@" + fqStageName + "/'" + " FILES="
                        + chunks.stream().map(chunk -> chunk.getChunk().getFileName()).map(name -> "'" + name + ".gz'")
                                .collect(joining(",", "(", ")"))
                        + " FILE_FORMAT=(TYPE=CSV field_delimiter=',' COMPRESSION=GZIP field_optionally_enclosed_by='\"')"
                        + " PURGE=TRUE" + " ON_ERROR='CONTINUE'")) {
            while (result.next()) {
                final String status = result.getString("status");
                if ("LOAD_FAILED".equals(status)) {
                    final String file = result.getString("file");
                    final String error = result.getString("FIRST_ERROR");
                    final int rowsLoaded = result.getInt("rows_loaded");
                    final int rowsParsed = result.getInt("rows_parsed");
                    errors.add(new CopyResult(file, error, rowsLoaded, rowsParsed));
                }
            }
        } catch (final SQLException e) {
            throw new IllegalStateException(e);
        }

        if (!errors.isEmpty()) {
            if (errors.size() == chunks.size()) {
                throw new IllegalStateException(errors.stream().map(CopyResult::getError).collect(joining("\n")));
            }

            return errors.stream()
                    .flatMap(e -> chunks.stream().filter(chunk -> chunk.getChunk().getFileName().toString().equals(e.getFile()))
                            .flatMap(chunk -> chunk.getRecords().stream().map(record -> new Reject(e.getError(), record))))
                    .collect(toList());
        }

        return emptyList();
    }

    @Data
    private static class CopyResult {

        private final String file;

        private final String error;

        private final int rowLoaded;

        private final int rowParsed;
    }

    private Collection<RecordChunk> splitRecords(final Path directoryPath, final List<Record> records) {
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
        return chunks.values();
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
    protected String buildQuery(List<Record> records) {
        return null;
    }

    @Override
    protected Map<Integer, Schema.Entry> getQueryParams() {
        return emptyMap();
    }

    @Override
    protected boolean validateQueryParam(Record record) {
        return true;
    }

    private static String valueOf(Record record, Schema.Entry entry) {
        switch (entry.getType()) {
        case INT:
            return String.valueOf(record.getInt(entry.getName()));
        case LONG:
            return String.valueOf(record.getLong(entry.getName()));
        case BYTES:
            return Hex.encodeHexString(record.getBytes(entry.getName()));
        case FLOAT:
            return String.valueOf(record.getFloat(entry.getName()));
        case DOUBLE:
            return String.valueOf(record.getDouble(entry.getName()));
        case BOOLEAN:
            return String.valueOf(record.getBoolean(entry.getName()));
        case DATETIME:
            return String.valueOf(record.getDateTime(entry.getName()).toInstant().toEpochMilli());
        case STRING:
            return "\"" + record.getString(entry.getName()) + "\"";
        case ARRAY:
        case RECORD:
        default:
            throw new IllegalArgumentException("unsupported type in " + entry);
        }
    }
}
