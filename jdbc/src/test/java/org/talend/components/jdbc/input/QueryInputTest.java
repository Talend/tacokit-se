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
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

public abstract class QueryInputTest extends BaseJdbcTest {

    @Test
    @DisplayName("QueryInput - valid query")
    void validQuery() {
        final int rowCount = getRandomRowCount();
        insertRows(rowCount, false, 0, null);
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection());
        dataset.setSqlQuery("select * from " + getTestTableName());
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = getComponentsHandler().getCollectedData(Record.class);
        assertEquals(rowCount, collectedData.size());
    }

    @Test
    @DisplayName("QueryInput - unvalid query ")
    void invalidQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection());
        dataset.setSqlQuery("select fromm " + getTestTableName());
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("QueryInput -  non authorized query (drop table)")
    void unauthorizedDropQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection());
        dataset.setSqlQuery("drop table " + getTestTableName());
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("QueryInput -  non authorized query (insert into)")
    void unauthorizedInsertQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(newConnection());
        dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }
}
