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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.jdbc.BaseJdbcTest;
import org.talend.components.jdbc.Disabled;
import org.talend.components.jdbc.DisabledDatabases;
import org.talend.components.jdbc.WithDatabasesEnvironments;
import org.talend.components.jdbc.containers.JdbcTestContainer;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.apache.derby.vti.XmlVTI.asList;
import static org.talend.components.jdbc.Database.SNOWFLAKE;

@DisplayName("Platforms")
@Environment(ContextualEnvironment.class)
@ExtendWith({ WithDatabasesEnvironments.class })
@DisabledDatabases({ @Disabled(value = SNOWFLAKE, reason = "Snowflake credentials need to be setup on ci") })
class PlatformTests extends BaseJdbcTest {

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    private final Date date = new Date(new SimpleDateFormat("yyyy-MM-dd").parse("2018-12-6").getTime());

    private final Date datetime = new Date();

    private final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39

    private List<Record> records = new ArrayList<>();

    PlatformTests() throws ParseException {
    }

    @BeforeEach
    void beforeEach() {
        if (records.isEmpty()) {
            records.add(recordBuilderFactory.newRecordBuilder().withInt("id", 1).withString("email", "user@talend.com")
                    .withString("t_text", RandomStringUtils.randomAlphabetic(300)).withBoolean("t_boolean", true)
                    .withLong("t_long", 10000000000L).withDouble("t_double", 1000.85d).withFloat("t_float", 15.50f)
                    .withDateTime("t_date", date).withDateTime("t_datetime", datetime).withDateTime("t_time", time)
                    .withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8)).build());
        }
    }

    @TestTemplate
    @DisplayName("Create table - Single primary key")
    void createTable(final TestInfo testInfo, final JdbcTestContainer container) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection dataStore = newConnection(container);
        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(dataStore).createTableIfNotExist(connection, testTable, asList("id"), -1, records);
            }
        }
    }

    @TestTemplate
    @DisplayName("Create table - Combined primary key")
    void createTableWithCombinedPrimaryKeys(final TestInfo testInfo, final JdbcTestContainer container) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection dataStore = newConnection(container);
        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(dataStore).createTableIfNotExist(connection, testTable, asList("id", "email"), -1, records);
            }
        }
    }

    @TestTemplate
    @DisplayName("Create table - existing table")
    void createExistingTable(final TestInfo testInfo, final JdbcTestContainer container) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection dataStore = newConnection(container);
        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
            try (final Connection connection = dataSource.getConnection()) {
                Platform platform = PlatformFactory.get(dataStore);
                platform.createTableIfNotExist(connection, testTable, asList("id", "email"), -1, records);
                // recreate the table should not fail
                platform.createTableIfNotExist(connection, testTable, asList("id", "email"), -1, records);
            }
        }
    }

}
