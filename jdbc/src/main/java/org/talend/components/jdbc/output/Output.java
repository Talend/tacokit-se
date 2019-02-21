/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.output;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.output.statement.JdbcActionFactory;
import org.talend.components.jdbc.output.statement.operations.JdbcAction;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputConfig configuration;

    private final JdbcService jdbcService;

    private final I18nMessage i18n;

    private transient List<Record> records;

    private transient JdbcAction jdbcAction;

    private transient JdbcService.JdbcDatasource dataSource;

    private transient Platform platform;

    private boolean tableExists;

    private boolean tableCreated;

    public Output(@Option("configuration") final OutputConfig outputConfig, final JdbcService jdbcService,
            final I18nMessage i18nMessage) {
        this.configuration = outputConfig;
        this.jdbcService = jdbcService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() throws SQLException {
        platform = PlatformFactory.get(configuration.getDataset().getConnection());
        dataSource = jdbcService.createDataSource(configuration.getDataset().getConnection(),
                configuration.isRewriteBatchedStatements());
        final JdbcActionFactory jdbcActionFactory = new JdbcActionFactory(platform, i18n, dataSource, configuration);
        this.jdbcAction = jdbcActionFactory.createAction();
        this.records = new ArrayList<>();
        try (final Connection connection = dataSource.getConnection()) {
            tableExists = checkTableExistence(configuration.getDataset().getTableName(), connection);
            if (!tableExists && !this.configuration.isCreateTableIfNotExists()) {
                throw new IllegalStateException(i18n.errorTaberDoesNotExists(configuration.getDataset().getTableName()));
            }
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        records.clear();
    }

    @ElementListener
    public void elementListener(@Input final Record record) {
        records.add(record);
    }

    @AfterGroup
    public void afterGroup() throws SQLException {
        if (!tableExists && !tableCreated && configuration.isCreateTableIfNotExists()) {
            try (final Connection connection = dataSource.getConnection()) {
                platform.createTableIfNotExist(connection, configuration.getDataset().getTableName(), configuration.getKeys(),
                        records);
                tableCreated = true;
            }
        }

        // TODO : handle discarded records
        try {
            final List<Reject> discards = jdbcAction.execute(records);
            discards.stream().map(Object::toString).forEach(log::error);
        } catch (final Throwable e) {
            records.stream().map(r -> new Reject(e.getMessage(), r)).map(Reject::toString).forEach(log::error);
            // unwrap sql exception to prevent class not found at runtime from the driver jar.
            // driver jar is loaded dynamically at runtime by this component and class from it may not be accessible form other
            // classloader
            if (SQLException.class.isAssignableFrom(e.getClass())) {
                throw (SQLException) e;
            } else {
                throw e;
            }
        }
    }

    private boolean checkTableExistence(final String tableName, final Connection connection) throws SQLException {
        try (final ResultSet resultSet = connection.getMetaData().getTables(connection.getCatalog(), connection.getSchema(),
                tableName, new String[] { "TABLE", "SYNONYM" })) {
            while (resultSet.next()) {
                if (ofNullable(ofNullable(resultSet.getString("TABLE_NAME")).orElseGet(() -> {
                    try {
                        return resultSet.getString("SYNONYM_NAME");
                    } catch (final SQLException e) {
                        return null;
                    }
                })).filter(tableName::equals).isPresent()) {
                    return true;
                }
            }
            return false;
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

}
