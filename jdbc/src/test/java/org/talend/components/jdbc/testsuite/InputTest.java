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
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.BeamDirectRunner;
import org.talend.components.jdbc.JdbcInvocationContextProvider;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("Input")
@WithComponents("org.talend.components.jdbc")
@ExtendWith({ JdbcInvocationContextProvider.class })
@Environment(ContextualEnvironment.class)
@Environment(BeamDirectRunner.class)
class InputTest extends BaseJdbcTest {

    @TestTemplate
    @DisplayName("Query - valid query")
    void validQuery(final JdbcTestContainer container) {
        final int rowCount = getRandomRowCount();
        insertRows(container, rowCount, false, 0, null);
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("select * from " + container.getTestTableName());
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @TestTemplate
    @DisplayName("Query - unvalid query ")
    void invalidQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("select fromm " + container.getTestTableName());
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @TestTemplate
    @DisplayName("Query -  non authorized query (drop table)")
    void unauthorizedDropQuery(final JdbcTestContainer container) {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection(container));
        dataset.setSqlQuery("drop table " + container.getTestTableName());
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
    void validTableName(final JdbcTestContainer container) {
        final int rowCount = getRandomRowCount();
        insertRows(container, rowCount, false, 0, null);
        final String config = configurationByExample().forInstance(newTableNameDataset(container.getTestTableName(), container))
                .configured().toQueryString();
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
