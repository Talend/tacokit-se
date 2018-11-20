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

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.statement.JdbcActionFactory;
import org.talend.components.jdbc.output.statement.operations.JdbcAction;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputConfiguration configuration;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    private transient Connection connection;

    private transient List<Record> records;

    private transient JdbcAction jdbcAction;

    public Output(@Option("configuration") final OutputConfiguration outputConfiguration, final JdbcService jdbcDriversService,
            final I18nMessage i18nMessage) {
        this.configuration = outputConfiguration;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        this.connection = jdbcDriversService.connection(configuration.getDataset().getConnection());
        this.jdbcAction = new JdbcActionFactory(i18n, this::getConnection, configuration).createAction();
        this.records = new ArrayList<>();
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
        // TODO : handle discarded records
        try {
            final List<Record> discards = jdbcAction.execute(records);
            discards.stream().map(Object::toString).forEach(r -> log.info("[rejected] - " + r));
        } catch (final Throwable e) {
            records.stream().map(Object::toString).forEach(r -> log.info("[rejected] - " + r));
            throw e;
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (connection != null) {
            try {
                connection.close();
            } catch (final SQLException e) {
                log.error(i18n.errorCantCloseJdbcConnectionProperly(), e);
                throw new IllegalStateException(e);
            }
        }
    }

    /**
     * Validate and recreate a connection if it's not valid
     *
     * @return a valid connection
     */
    private Connection getConnection() {
        try {
            if (this.connection == null) {
                this.connection = jdbcDriversService.connection(configuration.getDataset().getConnection());
            } else if (!jdbcDriversService.isConnectionValid(this.connection)) {
                log.debug("Invalid connection from connection pool. recreating a new one");
                try {
                    this.connection.close();
                } catch (final SQLException e) {
                    log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e);
                }
                this.connection = jdbcDriversService.connection(configuration.getDataset().getConnection());
            }
        } catch (final SQLException e) {
            log.debug("Can't validate connection state from connection pool. recreating a new one", e);
            try {
                this.connection.close();
            } catch (final SQLException e1) {
                log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e1);
            }

            this.connection = jdbcDriversService.connection(configuration.getDataset().getConnection());
        }

        try {
            this.connection.setAutoCommit(false);
        } catch (final SQLException e) {
            throw new IllegalStateException("can't deactivate auto commit for this database", e);
        }
        return this.connection;
    }
}
