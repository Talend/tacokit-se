package org.talend.components.jdbc.output.internal;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toMap;

import java.util.AbstractMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.sdk.component.api.record.Record;

public class DeleteStatementManager extends StatementManager {

    private final String tableName;

    private final String[] deleteKeys;

    private final Map<String, Integer> indexedColumns;

    public DeleteStatementManager(final OutputDataset dataset) {
        super();
        tableName = dataset.getTableName();
        deleteKeys = dataset.getDeleteKeys().stream().filter(Objects::nonNull).filter(key -> !key.isEmpty()).map(String::trim)
                .toArray(String[]::new);
        if (deleteKeys.length == 0) {
            throw new IllegalStateException("Please select the delete keys");
        }

        indexedColumns = IntStream.rangeClosed(1, deleteKeys.length)
                .mapToObj(i -> new AbstractMap.SimpleEntry<>(deleteKeys[i - 1], i))
                .collect(toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue));
    }

    @Override
    public String createQuery(final Record record) {
        return "DELETE FROM " + tableName + " WHERE " + Stream.of(deleteKeys).map(c -> c + " = ?").collect(joining(" AND "));
    }

    @Override
    public Map<String, Integer> getIndexedColumns() {
        return indexedColumns;
    }

}
