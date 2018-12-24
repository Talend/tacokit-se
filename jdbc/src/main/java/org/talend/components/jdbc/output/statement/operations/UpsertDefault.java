package org.talend.components.jdbc.output.statement.operations;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.RecordToSQLTypeConverter;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.*;

@Slf4j
@Getter
public class UpsertDefault extends QueryManager {

    private final Insert insert;

    private final Update update;

    private final List<String> keys;

    private Map<Integer, Schema.Entry> queryParams;

    public UpsertDefault(final Platform platform, final OutputConfig configuration, final I18nMessage i18n,
            final JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(i18n.errorNoKeyForUpdateQuery());
        }
        insert = new Insert(platform, configuration, i18n, dataSource);
        update = new Update(platform, configuration, i18n, dataSource);
    }

    @Override
    public String buildQuery(final List<Record> records) {
        this.queryParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());

        return "SELECT COUNT(*) AS RECORD_EXIST FROM " + getPlatform().identifier(getConfiguration().getDataset().getTableName())
                + " WHERE "
                + getConfiguration().getKeys().stream()
                        .peek(key -> queryParams.put(index.incrementAndGet(),
                                entries.stream().filter(e -> e.getName().equals(key)).findFirst()
                                        .orElseThrow(() -> new IllegalStateException(getI18n().errorNoFieldForQueryParam(key)))))
                        .map(c -> getPlatform().identifier(c)).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public boolean validateQueryParam(final Record record) {
        return record.getSchema().getEntries().stream().map(Schema.Entry::getName).collect(toSet())
                .containsAll(new HashSet<>(keys));
    }

    @Override
    public Map<Integer, Schema.Entry> getQueryParams() {
        return queryParams;
    }

    @Override
    public List<Reject> execute(final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return emptyList();
        }
        final List<Record> needUpdate = new ArrayList<>();
        final List<Record> needInsert = new ArrayList<>();
        final String query = buildQuery(records);
        final List<Reject> discards = new ArrayList<>();
        try (final Connection connection = getDataSource().getConnection()) {
            try (final PreparedStatement statement = connection.prepareStatement(query)) {
                for (final Record record : records) {
                    statement.clearParameters();
                    if (!validateQueryParam(record)) {
                        discards.add(new Reject("missing required query param in this record", record));
                        continue;
                    }
                    for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                        RecordToSQLTypeConverter.valueOf(entry.getValue().getType().name()).setValue(statement, entry.getKey(),
                                entry.getValue(), record);
                    }
                    try (final ResultSet result = statement.executeQuery()) {
                        if (result.next() && result.getInt("RECORD_EXIST") > 0) {
                            needUpdate.add(record);
                        } else {
                            needInsert.add(record);
                        }
                    }
                }
                connection.commit();
            } catch (final SQLException e) {
                connection.rollback();
                throw e;
            }
        }

        // fixme handle the update and insert in // need a pool of 2 !
        if (!needInsert.isEmpty()) {
            insert.buildQuery(needInsert);
            discards.addAll(insert.execute(needInsert));
        }
        if (!needUpdate.isEmpty()) {
            update.buildQuery(needUpdate);
            discards.addAll(update.execute(needUpdate));
        }

        return discards;
    }
}
