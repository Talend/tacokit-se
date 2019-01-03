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
package org.talend.components.jdbc;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static java.util.Optional.ofNullable;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Data
@Slf4j
public abstract class BaseJdbcTest {

    @Injected
    private BaseComponentsHandler componentsHandler;

    @Service
    private JdbcService jdbcService;

    @Service
    private UIActionService uiActionService;

    @Service
    private I18nMessage i18nMessage;

    public String getTestTableName(final TestInfo info) {
        return info.getTestClass().map(Class::getSimpleName).map(name -> name.substring(0, Math.min(5, name.length())))
                .orElse("TEST") + "_"
                + info.getTestMethod().map(Method::getName).map(name -> name.substring(0, Math.min(10, name.length())))
                        .orElse("TABLE");
    }

    @BeforeEach
    void beforeEach(final TestInfo testInfo, final JdbcTestContainer container) {
        dropTestTable(testInfo, container);
    }

    @AfterEach
    void afterEach(final TestInfo testInfo, final JdbcTestContainer container) {
        dropTestTable(testInfo, container);
    }

    private void dropTestTable(TestInfo testInfo, JdbcTestContainer container) {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection datastore = newConnection(container);
        final Platform platform = PlatformFactory.get(datastore);
        try (final Connection connection = jdbcService.createDataSource(datastore).getConnection()) {
            try (final PreparedStatement stm = connection.prepareStatement("DROP TABLE " + platform.identifier(testTable))) {
                stm.executeUpdate();
                connection.commit();
            } catch (final SQLException e) {
                connection.rollback();
                throw e;
            }
        } catch (final SQLException e) {
            log.trace("maybe table don't exists", e);
        }
    }

    public List<Record> readAll(final String table, final JdbcTestContainer container) {
        final InputTableNameConfig config = new InputTableNameConfig();
        config.setDataSet(newTableNameDataset(table, container));
        final String inConfig = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        final List<Record> data = new ArrayList<>(getComponentsHandler().getCollectedData(Record.class));
        getComponentsHandler().resetState();
        return data;
    }

    public long countAll(final String table, final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        final JdbcConnection connection = newConnection(container);
        dataset.setConnection(connection);
        final String total = "total";
        dataset.setSqlQuery("select count(*) as " + total + " from " + PlatformFactory.get(connection).identifier(table));
        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(dataset);
        final String inConfig = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        final Record data = getComponentsHandler().getCollectedData(Record.class).iterator().next();
        getComponentsHandler().resetState();
        return data.getSchema().getEntries().stream().filter(entry -> entry.getName().equalsIgnoreCase(total)).findFirst()
                .map(entry -> entry.getType() == Schema.Type.STRING ? Long.valueOf(data.getString(entry.getName()))
                        : data.getLong(entry.getName()))
                .orElse(0L);

    }

    public static void insertRows(final String table, final JdbcTestContainer container, final long rowCount,
            final boolean withNullValues, final int withMissingIdEvery, final String stringPrefix) {
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(table, container));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id"));
        configuration.setRewriteBatchedStatements(true);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?"
                                + rowGeneratorConfig(rowCount, withNullValues, withMissingIdEvery, stringPrefix))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
    }

    public static JdbcConnection newConnection(final JdbcTestContainer container) {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId(container.getUsername());
        connection.setPassword(container.getPassword());
        connection.setDbType(container.getDatabaseType());
        connection.setJdbcUrl(container.getJdbcUrl());
        return connection;
    }

    public static TableNameDataset newTableNameDataset(final String table, final JdbcTestContainer container) {
        TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection(container));
        dataset.setTableName(table);
        return dataset;
    }

    public static String rowGeneratorConfig(final long rowCount, final boolean withNullValues, final int withMissingIdEvery,
            final String stringPrefix) {
        return "config.rowCount=" + rowCount + "&config.withNullValues=" + withNullValues
                + ofNullable(stringPrefix).map(p -> "&config.stringPrefix=" + stringPrefix).orElse("")
                + "&config.withMissingIdEvery=" + withMissingIdEvery;
    }

}
