package org.talend.components.jdbc.output.statement;

import lombok.Data;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.platforms.*;
import org.talend.components.jdbc.output.statement.operations.*;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;

@Data
public class JdbcActionFactory {

    private final Platform platform;

    private final I18nMessage i18n;

    private final JdbcService.JdbcDatasource dataSource;

    private final OutputConfiguration configuration;

    public JdbcAction createAction() {
        final JdbcAction action;
        switch (configuration.getActionOnData()) {
        case INSERT:
            action = new Insert(platform, configuration, i18n, dataSource);
            break;
        case UPDATE:
            action = new Update(platform, configuration, i18n, dataSource);
            break;
        case DELETE:
            action = new Delete(platform, configuration, i18n, dataSource);
            break;
        case UPSERT:
            // todo : provide native upsert operation for every database
            switch (configuration.getDataset().getConnection().getDbType()) {
            case MySQLPlatform.NAME:
            case SnowflakePlatform.NAME:
            case OraclePlatform.NAME:
            case PostgreSQLPlatform.NAME:
            case DerbyPlatform.NAME:
            default:
                action = new UpsertDefault(platform, configuration, i18n, dataSource);
                break;
            }
            break;
        default:
            throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
        }

        return action;
    }

}
