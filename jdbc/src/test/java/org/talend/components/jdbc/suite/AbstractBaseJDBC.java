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
package org.talend.components.jdbc.suite;

import java.util.ArrayList;
import java.util.List;

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
import org.talend.components.jdbc.service.UIActionService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static java.util.Optional.ofNullable;
import static org.apache.derby.vti.XmlVTI.asList;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class AbstractBaseJDBC {

    @Service
    private I18nMessage i18nMessage;

    @Service
    private JdbcService jdbcService;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    @Service
    private UIActionService uiActionService;

    @Injected
    private BaseComponentsHandler componentsHandler;

    public I18nMessage getI18nMessage() {
        return i18nMessage;
    }

    public JdbcService getJdbcService() {
        return jdbcService;
    }

    public abstract JdbcTestContainer getContainer();

    public BaseComponentsHandler getComponentsHandler() {
        return componentsHandler;
    }

    public RecordBuilderFactory getRecordBuilderFactory() {
        return recordBuilderFactory;
    }

    public UIActionService getUiActionService() {
        return uiActionService;
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
        final Record data = componentsHandler.getCollectedData(Record.class).iterator().next();
        componentsHandler.resetState();
        return data.getSchema().getEntries().stream().filter(entry -> entry.getName().equalsIgnoreCase(total)).findFirst()
                .map(entry -> entry.getType() == Schema.Type.STRING ? Long.parseLong(data.getString(entry.getName()))
                        : data.getLong(entry.getName()))
                .orElse(0L);

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

    public List<Record> readAll(final String table, final BaseComponentsHandler componentsHandler) {
        final InputTableNameConfig config = new InputTableNameConfig();
        config.setDataSet(newTableNameDataset(table));
        final String inConfig = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();
        final List<Record> data = new ArrayList<>(componentsHandler.getCollectedData(Record.class));
        componentsHandler.resetState();
        return data;
    }
}
