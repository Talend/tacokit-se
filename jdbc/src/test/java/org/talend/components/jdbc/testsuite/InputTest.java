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

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.Disabled;
import org.talend.components.jdbc.DisabledDatabases;
import org.talend.components.jdbc.WithDatabasesEnvironments;
import org.talend.components.jdbc.configuration.InputAdvancedCommonConfig;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.configuration.InputTableNameConfig;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.DirectRunnerEnvironment;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.jdbc.Database.SNOWFLAKE;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Input")
@Environment(DirectRunnerEnvironment.class)
// @Environment(ContextualEnvironment.class)
@ExtendWith(WithDatabasesEnvironments.class)
@DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake credentials need to be setup on ci") })
class InputTest extends BaseJdbcTest {

    @TestTemplate
    @DisplayName("Query - valid select query")
    void validQuery(final TestInfo testInfo, final JdbcTestContainer container) {
        final int rowCount = 50;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        final SqlQueryDataset sqlQueryDataset = new SqlQueryDataset();
        final JdbcConnection connection = newConnection(container);
        sqlQueryDataset.setConnection(connection);
        sqlQueryDataset.setSqlQuery("select * from " + PlatformFactory.get(connection).identifier(testTableName));
        final InputQueryConfig config = new InputQueryConfig();
        config.setAdvancedCommonConfig(new InputAdvancedCommonConfig());
        config.setFetchSize(rowCount / 3);
        config.setDataSet(sqlQueryDataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @TestTemplate
    @DisplayName("Query - unvalid query ")
    void invalidQuery(final TestInfo testInfo, final JdbcTestContainer container) {
        final SqlQueryDataset sqlQueryDataset = new SqlQueryDataset();
        final JdbcConnection connection = newConnection(container);
        sqlQueryDataset.setConnection(connection);
        sqlQueryDataset.setSqlQuery("select fromm " + getTestTableName(testInfo));
        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(sqlQueryDataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @TestTemplate
    @DisplayName("Query -  non authorized query (drop table)")
    void unauthorizedDropQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("drop table abc");
        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(dataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalArgumentException.class,
                () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                        .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                        .run());
    }

    @TestTemplate
    @DisplayName("Query -  non authorized query (insert into)")
    void unauthorizedInsertQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(dataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalArgumentException.class,
                () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                        .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                        .run());
    }

    @TestTemplate
    @DisplayName("TableName - valid table name")
    void validTableName(final TestInfo testInfo, final JdbcTestContainer container) {
        final int rowCount = 50;
        final String testTableName = getTestTableName(testInfo);
        insertRows(testTableName, container, rowCount, false, 0, null);
        final InputTableNameConfig config = new InputTableNameConfig();
        config.setDataSet(newTableNameDataset(testTableName, container));
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + configURI).component("collector", "test://collector")
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
        final InputTableNameConfig config = new InputTableNameConfig();
        config.setDataSet(dataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalStateException.class,
                () -> Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + configURI)
                        .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build()
                        .run());
    }
}
