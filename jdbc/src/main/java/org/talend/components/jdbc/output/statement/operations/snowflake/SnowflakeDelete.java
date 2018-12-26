package org.talend.components.jdbc.output.statement.operations.snowflake;

import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.operations.Delete;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

import static java.time.LocalDateTime.now;
import static java.util.stream.Collectors.joining;
import static org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy.putAndCopy;
import static org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy.tmpTableName;

public class SnowflakeDelete extends Delete {

    public SnowflakeDelete(Platform platform, OutputConfig configuration, I18nMessage i18n,
            JdbcService.JdbcDatasource dataSource) {
        super(platform, configuration, i18n, dataSource);
    }

    @Override
    public List<Reject> execute(List<Record> records) throws SQLException {
        buildQuery(records);
        final List<Reject> rejects = new ArrayList<>();
        try (final Connection connection = getDataSource().getConnection()) {
            final String tableName = getConfiguration().getDataset().getTableName();
            final String tmpTableName = tmpTableName(tableName);
            final String fqTableName = namespace(connection) + "." + getPlatform().identifier(tableName);
            final String fqTmpTableName = namespace(connection) + "." + getPlatform().identifier(tmpTableName);
            final String fqStageName = namespace(connection) + ".%" + getPlatform().identifier(tmpTableName);
            rejects.addAll(putAndCopy(connection, records, fqStageName, fqTableName, fqTmpTableName));
            // delete from temp table
            try (final Statement statement = connection.createStatement()) {
                statement.execute("delete from " + fqTableName + " target using " + fqTmpTableName + " as source where "
                        + getConfiguration().getKeys().stream().map(key -> getPlatform().identifier(key))
                                .map(key -> "source." + key + "= target." + key).collect(joining("AND", " ", " ")));
            }
            connection.commit();
        }
        return rejects;
    }
}
