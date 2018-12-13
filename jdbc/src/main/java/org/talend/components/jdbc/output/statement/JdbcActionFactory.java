package org.talend.components.jdbc.output.statement;

import lombok.Data;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.*;
import org.talend.components.jdbc.output.statement.operations.*;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;

import static org.talend.components.jdbc.output.platforms.DerbyPlatform.DERBY;
import static org.talend.components.jdbc.output.platforms.MSSQLPlatform.MSSQL;
import static org.talend.components.jdbc.output.platforms.MariaDbPlatform.MARIADB;
import static org.talend.components.jdbc.output.platforms.MySQLPlatform.MYSQL;
import static org.talend.components.jdbc.output.platforms.OraclePlatform.ORACLE;
import static org.talend.components.jdbc.output.platforms.PostgreSQLPlatform.POSTGRESQL;
import static org.talend.components.jdbc.output.platforms.RedshiftPlatform.REDSHIFT;
import static org.talend.components.jdbc.output.platforms.SnowflakePlatform.SNOWFLAKE;

@Data
public class JdbcActionFactory {

    private final Platform platform;

    private final I18nMessage i18n;

    private final JdbcService.JdbcDatasource dataSource;

    private final OutputConfig configuration;

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
            case MYSQL:
            case SNOWFLAKE:
            case ORACLE:
            case POSTGRESQL:
            case MARIADB:
            case REDSHIFT:
            case MSSQL:
            case DERBY:
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
