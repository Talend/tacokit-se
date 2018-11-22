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
package org.talend.components.jdbc.input;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class TableNameInputTest extends BaseJdbcTest {

    @Test
    @DisplayName("TableNameInput - valid table name")
    void validTableName() {
        final int rowCount = getRandomRowCount();
        insertRows(rowCount, false, 0, null);
        final String config = configurationByExample().forInstance(newTableNameDataset(getTestTableName())).configured()
                .toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @Test
    @DisplayName("TableNameInput - invalid table name")
    void invalidTableName() {
        final TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(newConnection());
        dataset.setTableName("xxx");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("TableNameInput - missing driver")
    void missingDriverConfig() {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLEXX");
        connection.setJdbcUrl("jdbc:DDL://localhost:1234/foo");
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("TableNameInput - missing driver file")
    void missingDriverFile() {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLE");
        connection.setJdbcUrl("jdbc:DDL://localhost:1234/foo");
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class,
                () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                        .component("collector", "jdbcTest://DataCollector").connections().from("jdbcInput").to("collector")
                        .build().run());
    }
}
