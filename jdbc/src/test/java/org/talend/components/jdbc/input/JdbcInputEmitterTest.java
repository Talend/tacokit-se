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

import static java.util.stream.Collectors.toSet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.BaseTest;
import org.talend.components.jdbc.DerbyExtension;
import org.talend.components.jdbc.WithDerby;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@WithDerby(onStartSQLScript = "derby/input_create.sql", onShutdownSQLScript = "derby/delete.sql")
@WithComponents("org.talend.components.jdbc") // component package
class JdbcInputEmitterTest extends BaseTest {

    @Service
    private JdbcService jdbcService;

    private BasicDatastore datastore;

    @BeforeEach
    void clearTable(final DerbyExtension.DerbyInfo derbyInfo) {
        datastore = newConnection(derbyInfo);
    }

    @Test
    @DisplayName("Execute a valid query")
    void validQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(datastore);
        dataset.setSqlQuery("select * from users");

        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = componentsHandler.getCollectedData(Record.class);
        assertEquals(4, collectedData.size());
        assertEquals(Stream.of("user1", "user2", "user3", "user4").collect(toSet()),
                collectedData.stream().map(r -> r.getString("NAME")).collect(toSet()));
    }

    @Test
    @DisplayName("Execute an unvalid query ")
    void invalidQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(datastore);
        dataset.setSqlQuery("select from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute a non authorized query (drop table)")
    void unauthorizedDropQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(datastore);
        dataset.setSqlQuery("drop table users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute a non authorized query (insert into)")
    void unauthorizedInsertQuery() {
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(datastore);
        dataset.setSqlQuery("INSERT INTO users(id, name) VALUES (1, 'user1')");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalArgumentException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using valid table name")
    void validTableName() {
        final TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(datastore);
        dataset.setTableName("users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config).component("collector", "test://collector")
                .connections().from("jdbcInput").to("collector").build().run();

        final List<Record> collectedData = componentsHandler.getCollectedData(Record.class);
        assertEquals(4, collectedData.size());
        assertEquals(Stream.of("user1", "user2", "user3", "user4").collect(toSet()),
                collectedData.stream().map(r -> r.getString("NAME")).collect(toSet()));
    }

    @Test
    @DisplayName("Execute query using invalid table name")
    void invalidTableName() {
        final TableNameDataset dataset = new TableNameDataset();
        dataset.setConnection(datastore);
        dataset.setTableName("xxx");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://TableNameInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using missing driver")
    void missingDriverConfig() {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLEXX");
        connection.setJdbcUrl("jdbc:derby://localhost:1234/foo");
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");
        final String config = configurationByExample().forInstance(dataset).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + config)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("Execute query using missing driver file")
    void missingDriverFile() {
        final BasicDatastore connection = new BasicDatastore();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLE");
        connection.setJdbcUrl("jdbc:derby://localhost:1234/foo");
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
