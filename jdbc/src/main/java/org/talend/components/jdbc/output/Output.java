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

import static java.util.Optional.ofNullable;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.output.internal.StatementManager;
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
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputConfiguration outputConfiguration;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    private transient StatementManager statementManager;

    private transient int initPoolSize = 1;

    private transient List<Connection> connectionPool;

    private transient List<Connection> usedConnections;

    public Output(@Option("configuration") final OutputConfiguration outputConfiguration, final JdbcService jdbcDriversService,
            final I18nMessage i18nMessage) {
        this.outputConfiguration = outputConfiguration;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        connectionPool = new ArrayList<>();
        usedConnections = new ArrayList<>();
        for (int i = 0; i < initPoolSize; i++) {
            connectionPool.add(getConnection());
        }
    }

    @BeforeGroup
    public void beforeGroup() {
        this.statementManager = StatementManager.get(outputConfiguration, i18n, getConnection());
    }

    @ElementListener
    public void elementListener(@Input final Record record) {
        statementManager.addBatch(record);
    }

    @AfterGroup
    public void afterGroup() {
        statementManager.executeBatch();
        statementManager.close();
        releaseConnection(statementManager.getConnection());
    }

    @PreDestroy
    public void preDestroy() {
        connectionPool.forEach(connection -> {
            try {
                connection.close();
            } catch (SQLException e) {
                log.error(i18n.errorCantCloseJdbcConnectionProperly(), e);
            }
        });
    }

    private Connection getConnection() {
        final Connection connection = new ArrayList<>(connectionPool).stream().filter(c -> {
            connectionPool.remove(c);
            try {
                if (!jdbcDriversService.isConnectionValid(c)) {
                    log.debug("Invalid connection from connection pool. recreating a new one");
                    try {
                        c.close();
                    } catch (final SQLException e) {
                        log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e);
                    }
                }
                return true;
            } catch (final SQLException e) {
                log.debug("Can't validate connection state from connection pool. recreating a new one", e);
                try {
                    c.close();
                } catch (SQLException e1) {
                    log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e1);
                }
            }
            return false;
        }).findFirst().orElseGet(() -> {
            final Connection c = jdbcDriversService.connection(outputConfiguration.getDataset().getConnection());
            try {
                c.setAutoCommit(false);
            } catch (SQLException e) {
                log.error("Can't deactivate auto-commit, this may alter the performance if this batch");
            }
            return c;
        });
        usedConnections.add(connection);
        return connection;
    }

    private void releaseConnection(final Connection connection) {
        if (usedConnections.remove(connection)) {
            connectionPool.add(connection);
        } else {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn(i18n.errorCantCloseJdbcConnectionProperly(), e);
            }
        }
    }
}
