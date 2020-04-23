/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.suite;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import static java.util.Optional.ofNullable;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@TestInstance(Lifecycle.PER_CLASS)
public abstract class JDBCBaseTest {

    private JdbcTestContainer container = null;

    private DataSource dataSource = null;

    @Service
    private I18nMessage i18nMessage;

    @Service
    private JdbcService jdbcService;

    @Injected
    private BaseComponentsHandler componentsHandler;

    public DataSource getDataSource() {
        if (this.dataSource == null) {
            this.dataSource = this.buildDataSource(this.getContainer());
        }
        return this.dataSource;
    }

    public JdbcTestContainer getContainer() {
        if (this.container == null) {
            this.container = this.buildContainer();
        }
        return this.container;
    }

    @BeforeAll
    public void init() {
        System.out.println(System.currentTimeMillis());
        JdbcTestContainer container = this.getContainer();
        container.start();
    }

    @AfterAll
    public void release() {
        this.getContainer().close();
        System.out.println(System.currentTimeMillis());
    }

    private DataSource buildDataSource(JdbcTestContainer container) {
        final HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());

        final HikariDataSource dataSource = new HikariDataSource(hikariConfig);

        return dataSource;
    }

    public JdbcConnection newConnection() {
        final JdbcConnection connection = new JdbcConnection();
        final JdbcTestContainer container = this.getContainer();
        connection.setUserId(container.getUsername());
        connection.setPassword(container.getPassword());
        connection.setDbType(container.getDatabaseType());
        connection.setJdbcUrl(container.getJdbcUrl());
        return connection;
    }

    public void insertRows(final String table, final long rowCount, final boolean withNullValues, final String stringPrefix) {
        final JdbcTestContainer container = this.getContainer();
        final boolean withBoolean = !container.getDatabaseType().equalsIgnoreCase("oracle");
        final boolean withBytes = !container.getDatabaseType().equalsIgnoreCase("redshift");
        final OutputConfig configuration = new OutputConfig();
        configuration.setDataset(newTableNameDataset(table));
        configuration.setActionOnData(OutputConfig.ActionOnData.INSERT.name());
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id"));
        configuration.setRewriteBatchedStatements(true);
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components()
                .component("rowGenerator",
                        "jdbcTest://RowGenerator?"
                                + rowGeneratorConfig(rowCount, withNullValues, stringPrefix, withBoolean, withBytes))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();
    }

    public TableNameDataset newTableNameDataset(final String table) {
        TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection());
        dataset.setTableName(table);
        return dataset;
    }

    public String rowGeneratorConfig(final long rowCount, final boolean withNullValues, final String stringPrefix,
            final boolean withBoolean, final boolean withBytes) {
        return "config.rowCount=" + rowCount // row count
                + "&config.withNullValues=" + withNullValues // with null
                + ofNullable(stringPrefix).map(p -> "&config.stringPrefix=" + stringPrefix).orElse("") //
                + "&config.withBoolean=" + withBoolean //
                + "&config.withBytes=" + withBytes; //
    }

    public String getTestTableName(final TestInfo info) {
        return info.getTestClass().map(Class::getSimpleName).map(name -> name.substring(0, Math.min(5, name.length())))
                .orElse("TEST") + "_"
                + info.getTestMethod().map(Method::getName).map(name -> name.substring(0, Math.min(10, name.length())))
                        .orElse("TABLE");
    }

    public long countAll(final String table) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        final JdbcConnection connection = newConnection();
        dataset.setConnection(connection);
        final String total = "total";
        dataset.setSqlQuery(
                "select count(*) as " + total + " from " + PlatformFactory.get(connection, i18nMessage).identifier(table));
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

    public List<Record> readAll(final String table) {
        final InputTableNameConfig config = new InputTableNameConfig();
        config.setDataSet(newTableNameDataset(table));
        final String inConfig = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        final List<Record> data = new ArrayList<>(getComponentsHandler().getCollectedData(Record.class));
        getComponentsHandler().resetState();
        return data;
    }

    public abstract JdbcTestContainer buildContainer();

    public I18nMessage getI18nMessage() {
        return i18nMessage;
    }

    public void setI18nMessage(I18nMessage i18nMessage) {
        this.i18nMessage = i18nMessage;
    }

    public JdbcService getJdbcService() {
        return jdbcService;
    }

    public void setJdbcService(JdbcService jdbcService) {
        this.jdbcService = jdbcService;
    }

    public BaseComponentsHandler getComponentsHandler() {
        return componentsHandler;
    }

    public void setComponentsHandler(BaseComponentsHandler componentsHandler) {
        this.componentsHandler = componentsHandler;
    }
}
