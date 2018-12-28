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
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.output.statement.operations.QueryManager;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.talend.components.jdbc.output.statement.QueryManagerFactory.getQueryManager;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.DATASTORE)
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputConfig configuration;

    private final JdbcService jdbcService;

    private final I18nMessage i18n;

    private transient List<Record> records;

    private transient QueryManager queryManager;

    private transient JdbcService.JdbcDatasource datasource;

    private transient Platform platform;

    private Boolean tableExistsCheck;

    private boolean tableCreated;

    private transient boolean init;

    public Output(@Option("configuration") final OutputConfig outputConfig, final JdbcService jdbcService,
            final I18nMessage i18nMessage) {
        this.configuration = outputConfig;
        this.jdbcService = jdbcService;
        this.i18n = i18nMessage;
    }

    @BeforeGroup
    public void beforeGroup() throws SQLException {
        this.records = new ArrayList<>();
    }

    @ElementListener
    public void elementListener(@Input final Record record) throws SQLException {
        if (!init) {
            // prevent creating db connection if no records
            // it's mostly useful for streaming scenario
            lazyInit();
        }
        records.add(record);
    }

    private void lazyInit() throws SQLException {
        this.init = true;
        final JdbcConnection connection = configuration.getDataset().getConnection();
        this.platform = PlatformFactory.get(connection);
        this.datasource = jdbcService.createDataSource(connection, configuration.isRewriteBatchedStatements());
        this.queryManager = getQueryManager(platform, i18n, configuration, datasource);
        if (this.tableExistsCheck == null) {
            this.tableExistsCheck = this.queryManager.checkTableExistence(configuration.getDataset().getTableName());
        }
        if (!this.tableExistsCheck && !this.configuration.isCreateTableIfNotExists()) {
            throw new IllegalStateException(this.i18n.errorTaberDoesNotExists(this.configuration.getDataset().getTableName()));
        }
    }

    @AfterGroup
    public void afterGroup() throws SQLException {
        if (!tableExistsCheck && !tableCreated && configuration.isCreateTableIfNotExists()) {
            try (final Connection connection = datasource.getConnection()) {
                platform.createTableIfNotExist(connection, configuration.getDataset().getTableName(), configuration.getKeys(),
                        records);
                tableCreated = true;
            }
        }

        // TODO : handle discarded records
        try {
            final List<Reject> discards = queryManager.execute(records);
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

    @PreDestroy
    public void preDestroy() {
        if (datasource != null) {
            datasource.close();
        }
    }

}
