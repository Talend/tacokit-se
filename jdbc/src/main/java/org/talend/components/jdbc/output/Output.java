/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.statement.QueryManager;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static org.talend.components.jdbc.ErrorFactory.toIllegalStateException;
import static org.talend.components.jdbc.service.JdbcService.checkTableExistence;

@Slf4j
public abstract class Output implements Serializable {

    private final OutputConfig configuration;

    @Getter
    private final JdbcService jdbcService;

    private final I18nMessage i18n;

    private transient List<Record> records;

    private transient JdbcService.JdbcDatasource datasource;

    private Boolean tableExistsCheck;

    private boolean tableCreated;

    private transient boolean init;

    public Output(final OutputConfig outputConfig, final JdbcService jdbcService, final I18nMessage i18nMessage) {
        this.configuration = outputConfig;
        this.jdbcService = jdbcService;
        this.i18n = i18nMessage;
    }

    protected abstract QueryManager getQueryManager();

    protected abstract Platform getPlatform();

    @BeforeGroup
    public void beforeGroup() {
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
        this.datasource = jdbcService.createDataSource(configuration.getDataset().getConnection(),
                configuration.isRewriteBatchedStatements());
        if (this.tableExistsCheck == null) {
            this.tableExistsCheck = checkTableExistence(configuration.getDataset().getTableName(), datasource);
        }
        if (!this.tableExistsCheck && !this.configuration.isCreateTableIfNotExists()) {
            throw new IllegalStateException(this.i18n.errorTaberDoesNotExists(this.configuration.getDataset().getTableName()));
        }
    }

    @AfterGroup
    public void afterGroup() throws SQLException {
        if (!tableExistsCheck && !tableCreated && configuration.isCreateTableIfNotExists()) {
            try (final Connection connection = datasource.getConnection()) {
                getPlatform().createTableIfNotExist(connection, configuration.getDataset().getTableName(),
                        configuration.getKeys(), configuration.getSortStrategy(), configuration.getSortKeys(),
                        configuration.getDistributionStrategy(), configuration.getDistributionKeys(),
                        configuration.getVarcharLength(), records);
                tableCreated = true;
            }
        }

        // TODO : handle discarded records
        try {
            final List<Reject> discards = getQueryManager().execute(records, datasource);
            discards.stream().map(Object::toString).forEach(log::error);
        } catch (final SQLException | IOException e) {
            records.stream().map(r -> new Reject(e.getMessage(), r)).map(Reject::toString).forEach(log::error);
            throw toIllegalStateException(e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (datasource != null) {
            datasource.close();
        }
    }

}
