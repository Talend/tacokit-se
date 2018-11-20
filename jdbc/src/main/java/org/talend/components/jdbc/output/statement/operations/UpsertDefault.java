package org.talend.components.jdbc.output.statement.operations;

import static java.util.Collections.emptyList;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.statement.RecordToSQLTypeConverter;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class UpsertDefault extends JdbcAction {

    private final Insert insert;

    private final Update update;

    private final List<String> keys;

    private Map<Integer, Schema.Entry> queryParams;

    public UpsertDefault(final OutputConfiguration configuration, final I18nMessage i18n, final Supplier<Connection> connection) {
        super(configuration, i18n, connection);
        this.keys = new ArrayList<>(ofNullable(configuration.getKeys()).orElse(emptyList()));
        if (this.keys.isEmpty()) {
            throw new IllegalArgumentException(i18n.errorNoKeyForUpdateQuery());
        }
        insert = new Insert(configuration, i18n, connection);
        update = new Update(configuration, i18n, connection);
    }

    @Override
    public String buildQuery(final List<Record> records) {
        this.queryParams = new HashMap<>();
        final AtomicInteger index = new AtomicInteger(0);
        final List<Schema.Entry> entries = records.stream().flatMap(r -> r.getSchema().getEntries().stream()).distinct()
                .collect(toList());

        return "SELECT COUNT(*) AS RECORD_EXIST FROM " + getConfiguration().getDataset().getTableName() + " WHERE "
                + getConfiguration().getKeys().stream()
                        .peek(key -> queryParams.put(index.incrementAndGet(),
                                entries.stream().filter(e -> e.getName().equals(key)).findFirst()
                                        .orElseThrow(() -> new IllegalStateException(getI18n().errorNoFieldForQueryParam(key)))))
                        .map(c -> c + " = ?").collect(joining(" AND "));
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
    public List<Record> execute(final List<Record> records) {
        if (records.isEmpty()) {
            return emptyList();
        }
        final List<Record> needUpdate = new ArrayList<>();
        final List<Record> needInsert = new ArrayList<>();
        final String query = buildQuery(records);
        final Connection connection = getConnection().get();
        final List<Record> discards = new ArrayList<>();
        try (final PreparedStatement statement = connection.prepareStatement(query)) {
            for (final Record record : records) {
                statement.clearParameters();
                if (!validateQueryParam(record)) {
                    discards.add(record);
                    continue;
                }
                for (final Map.Entry<Integer, Schema.Entry> entry : getQueryParams().entrySet()) {
                    RecordToSQLTypeConverter.valueOf(entry.getValue().getType().name()).setValue(statement, entry.getKey(),
                            entry.getValue().getName(), record);
                }
                try (final ResultSet result = statement.executeQuery()) {
                    if (result.next() && result.getInt("RECORD_EXIST") > 0) {
                        needUpdate.add(record);
                    } else {
                        needInsert.add(record);
                    }
                }
            }
        } catch (final SQLException e) {
            throw new RuntimeException(e);
        }
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
