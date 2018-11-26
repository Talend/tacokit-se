package org.talend.components.jdbc.output.statement;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.statement.operations.*;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;

@Data
public class JdbcActionFactory {

    private final I18nMessage i18n;

    private final HikariDataSource dataSource;

    private final OutputConfiguration configuration;

    public JdbcAction createAction() {
        final JdbcAction action;
        switch (configuration.getActionOnData()) {
        case INSERT:
            action = new Insert(configuration, i18n, dataSource);
            break;
        case UPDATE:
            action = new Update(configuration, i18n, dataSource);
            break;
        case DELETE:
            action = new Delete(configuration, i18n, dataSource);
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
                action = new UpsertDefault(configuration, i18n, dataSource);
                break;
            }
            break;
        default:
            throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
        }

        return action;
    }

}
