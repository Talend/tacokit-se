package org.talend.components.jdbc.output.statement;

import lombok.Data;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.operations.*;
import org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeDelete;
import org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeInsert;
import org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeUpdate;
import org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeUpsert;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;

import static java.util.Locale.ROOT;
import static org.talend.components.jdbc.output.platforms.SnowflakePlatform.SNOWFLAKE;

@Data
public final class QueryManagerFactory {

    private QueryManagerFactory() {
    }

    public static QueryManager getQueryManager(final Platform platform, final I18nMessage i18n, final OutputConfig configuration,
            final JdbcService.JdbcDatasource dataSource) {
        final String db = configuration.getDataset().getConnection().getDbType().toLowerCase(ROOT);
        switch (configuration.getActionOnData()) {
        case INSERT:
            switch (db) {
            case SNOWFLAKE:
                return new SnowflakeInsert(platform, configuration, i18n, dataSource);
            default:
                return new Insert(platform, configuration, i18n, dataSource);
            }
        case UPDATE:
            switch (db) {
            case SNOWFLAKE:
                return new SnowflakeUpdate(platform, configuration, i18n, dataSource);
            default:
                return new Update(platform, configuration, i18n, dataSource);
            }
        case DELETE:
            switch (db) {
            case SNOWFLAKE:
                return new SnowflakeDelete(platform, configuration, i18n, dataSource);
            default:
                return new Delete(platform, configuration, i18n, dataSource);
            }
        case UPSERT:
            switch (db) {
            case SNOWFLAKE:
                return new SnowflakeUpsert(platform, configuration, i18n, dataSource);
            default:
                return new UpsertDefault(platform, configuration, i18n, dataSource);
            }
        default:
            throw new IllegalStateException(i18n.errorUnsupportedDatabaseAction());
        }
    }

}
