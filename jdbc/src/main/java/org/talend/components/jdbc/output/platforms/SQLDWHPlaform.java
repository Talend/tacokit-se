package org.talend.components.jdbc.output.platforms;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

@Slf4j
public class SQLDWHPlaform extends MSSQLPlatform {

    public static final String SQLDWH = "sqldwh";

    public SQLDWHPlaform(I18nMessage i18n) {
        super(i18n);
    }

    public void createTableIfNotExist(final Connection connection, final String name, final List<String> keys,
            final List<String> sortKeys, final DistributionStrategy distributionStrategy, final List<String> distributionKeys,
            final int varcharLength, final List<Record> records) throws SQLException {
        if (records.isEmpty()) {
            return;
        }

        final String sql = buildQuery(
                getTableModel(connection, name, keys, sortKeys, distributionStrategy, distributionKeys, varcharLength, records));
        try (final Statement statement = connection.createStatement()) {
            statement.executeUpdate(sql);
            connection.commit();
        } catch (final Throwable e) {
            if (!isTableExistsCreationError(e)) {
                throw e;
            }

            log.trace("create table issue was ignored. The table and it's name space has been created by an other worker", e);
        }
    }
}
