/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.jdbc.output.statement.operations.snowflake;

import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.Reject;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.operations.Insert;
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
import java.util.stream.Collectors;

import static java.time.LocalDateTime.now;
import static org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy.putAndCopy;
import static org.talend.components.jdbc.output.statement.operations.snowflake.SnowflakeCopy.tmpTableName;

public class SnowflakeInsert extends Insert {

    public SnowflakeInsert(Platform platform, OutputConfig configuration, I18nMessage i18n,
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
            try (final Statement statement = connection.createStatement()) {
                statement.execute("insert into " + fqTableName + getQueryParams().values().stream()
                        .map(e -> getPlatform().identifier(e.getName())).collect(Collectors.joining(",", "(", ")"))
                        + " select * from " + fqTmpTableName);
            }
            connection.commit();
        }
        return rejects;
    }

}
