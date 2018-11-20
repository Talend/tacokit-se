package org.talend.components.jdbc.output.statement;

import java.sql.Connection;
import java.util.function.Supplier;

import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.statement.operations.Delete;
import org.talend.components.jdbc.output.statement.operations.Insert;
import org.talend.components.jdbc.output.statement.operations.JdbcAction;
import org.talend.components.jdbc.output.statement.operations.Update;
import org.talend.components.jdbc.output.statement.operations.UpsertDefault;
import org.talend.components.jdbc.service.I18nMessage;

import lombok.Data;

@Data
public class StatementExecutorFactory {

    private final I18nMessage i18n;

    private final Supplier<Connection> connection;

    private final OutputConfiguration configuration;

    public JdbcAction createAction() {
        final JdbcAction action;
        switch (configuration.getActionOnData()) {
        case INSERT:
            action = new Insert(configuration, i18n, connection);
            break;
        case UPDATE:
            action = new Update(configuration, i18n, connection);
            break;
        case DELETE:
            action = new Delete(configuration, i18n, connection);
            break;
        case UPSERT:
            // todo : provide native upsert operation for every database
            switch (configuration.getDataset().getConnection().getDbType()) {
            case "Redshift":
            case "MySQL":
            case "Snowflake":
            case "Oracle":
            case "Postgresql":
            case "Derby":
            default:
                action = new UpsertDefault(configuration, i18n, connection);
                break;
            }
            break;
        default:
            throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
        }

        return action;
    }

}
