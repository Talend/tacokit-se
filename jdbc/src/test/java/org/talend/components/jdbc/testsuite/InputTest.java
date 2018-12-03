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
package org.talend.components.jdbc.testsuite;

import org.junit.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.Disabled;
import org.talend.components.jdbc.DisabledDatabases;
import org.talend.components.jdbc.JdbcInvocationContextProvider;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.apache.derby.vti.XmlVTI.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.jdbc.Database.SNOWFLAKE;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Input")
@ExtendWith({ JdbcInvocationContextProvider.class })
@Environment(ContextualEnvironment.class)
@Environment(DirectRunnerEnvironment.class)
@WithComponents("org.talend.components.jdbc")
@DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "need to be setup on ci") })
class InputTest extends BaseJdbcTest {

    @TestTemplate
    @DisplayName("Query - valid query")
    void validQuery(final TestInfo testInfo, final JdbcTestContainer container) {
        final int rowCount = getRandomRowCount();
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("select * from " + testTableName);
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @Test
    @DisplayName("redshift - valid query")
    void redshift(final TestInfo testInfo) {
        final int rowCount = 2;
        final String testTableName = getTestTableName(testInfo);

        final JdbcConnection connection = new JdbcConnection();
        connection.setDbType("Redshift");
        connection.setUserId("talend");
        connection.setPassword("Talend2018!!");
        connection.setJdbcUrl("jdbc:redshift://redshift-cluster-jdbc.cmdij9hodzia.us-east-2.redshift.amazonaws.com:5439/dev");

        final TableNameDataset tableNameDataSet = new TableNameDataset();
        tableNameDataSet.setConnection(connection);
        tableNameDataSet.setTableName(testTableName);

        final OutputConfiguration configuration = new OutputConfiguration();
        configuration.setDataset(tableNameDataSet);
        configuration.setActionOnData(OutputConfiguration.ActionOnData.INSERT);
        configuration.setCreateTableIfNotExists(true);
        configuration.setKeys(asList("id"));
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("rowGenerator", "jdbcTest://RowGenerator?" + rowGeneratorConfig(rowCount, false, 0, null))
                .component("jdbcOutput", "Jdbc://Output?" + config).connections().from("rowGenerator").to("jdbcOutput").build()
                .run();

        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from " + testTableName);
        final String inConfig = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + inConfig).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @TestTemplate
    @DisplayName("Query - unvalid query ")
    void invalidQuery(final TestInfo testInfo, final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("select fromm " + getTestTableName(testInfo));
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @TestTemplate
    @DisplayName("Query -  non authorized query (drop table)")
    void unauthorizedDropQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("drop table abc");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @TestTemplate
    @DisplayName("Query -  non authorized query (insert into)")
    void unauthorizedInsertQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @TestTemplate
    @DisplayName("TableName - valid table name")
    void validTableName(final TestInfo testInfo, final JdbcTestContainer container) {
        final int rowCount = getRandomRowCount();
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        final String config = configurationByExample().forInstance(newTableNameDataset(testTableName, container)).configured()
                .toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @TestTemplate
    @DisplayName("TableName - invalid table name")
    void invalidTableName(final JdbcTestContainer container) {
        final TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection(container));
        dataset.setTableName("xxx");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }
}
