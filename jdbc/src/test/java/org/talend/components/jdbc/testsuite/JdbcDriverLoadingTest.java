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
package org.talend.components.jdbc.testsuite;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.dataset.SqlQueryDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@DisplayName("JdbcDriverLoading")
@WithComponents("org.talend.components.jdbc")
class JdbcDriverLoadingTest {

    @Test
    @DisplayName("JdbcDriverLoading - missing driver")
    void missingDriverConfig() {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLEXX");
        connection.setJdbcUrl("jdbc:DDL://localhost:1234/foo");
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");

        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(dataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalStateException.class, () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                .component("collector", "test://collector").connections().from("jdbcInput").to("collector").build().run());
    }

    @Test
    @DisplayName("JdbcDriverLoading - missing driver file")
    void missingDriverFile() {
        final JdbcConnection connection = new JdbcConnection();
        connection.setUserId("sa");
        connection.setPassword("sa");
        connection.setDbType("ORACLE");
        connection.setJdbcUrl("jdbc:DDL://localhost:1234/foo");
        final SqlQueryDataset dataset = new SqlQueryDataset();
        dataset.setConnection(connection);
        dataset.setSqlQuery("select * from users");

        final InputQueryConfig config = new InputQueryConfig();
        config.setDataSet(dataset);
        final String configURI = configurationByExample().forInstance(config).configured().toQueryString();
        assertThrows(IllegalStateException.class,
                () -> Job.components().component("jdbcInput", "Jdbc://QueryInput?" + configURI)
                        .component("collector", "jdbcTest://DataCollector").connections().from("jdbcInput").to("collector")
                        .build().run());
    }
}