package org.talend.components.jdbc.output.internal;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.sdk.component.api.record.Record;

import lombok.Data;

@Data
public abstract class StatementManager {

    public abstract String createQuery(final Record record);

    public abstract Map<String, Integer> getIndexedColumns();

    public static StatementManager get(final OutputDataset dataset) {
        switch (dataset.getActionOnData()) {
        case Insert:
            return new InsertStatementManager(dataset);
        case Update:
            return new UpdateStatementManager(dataset);
        case Delete:
            return new DeleteStatementManager(dataset);
        default:
            throw new IllegalStateException("Unsupported operation");
        }
    }

    public void populateParameters(final PreparedStatement statement, final Record record) {
        getIndexedColumns().forEach((column, index) -> {
            try {
                record.getSchema().getEntries().stream().filter(e -> e.getName().equals(column)).findFirst()
                        .map(type -> RecordSQLTypes.valueOf(type.getType().name()))
                        .orElseThrow(() -> new IllegalStateException("Can't find a mapping value for the column '" + column
                                + "' in the incoming record '" + record.toString() + "'"))
                        .setValue(statement, index, column, record);
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
    }
}
