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

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;
import org.testcontainers.shaded.org.apache.commons.lang.RandomStringUtils;

import static java.util.Collections.emptyList;
import static org.apache.derby.vti.XmlVTI.asList;

@DisplayName("Platforms")
public abstract class PlatformTests extends JDBCBaseTest {

    private final RecordBuilderFactory recordBuilderFactory = new RecordBuilderFactoryImpl("test");

    private final ZonedDateTime date = ZonedDateTime.of(LocalDateTime.of(2018, 12, 6, 12, 0, 0), ZoneId.systemDefault());

    private final Date datetime = new Date();

    private final Date time = new Date(1000 * 60 * 60 * 15 + 1000 * 60 * 20 + 39000); // 15:20:39

    private List<Record> records;

    @BeforeEach
    void beforeEach() {
        records = new ArrayList<>();
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder().withInt("id", 1)
                .withString("email", "user@talend.com").withString("t_text", RandomStringUtils.randomAlphabetic(300))
                .withLong("t_long", 10000000000L).withDouble("t_double", 1000.85d).withFloat("t_float", 15.50f)
                .withDateTime("t_date", date).withDateTime("t_datetime", datetime).withDateTime("t_time", time);

        if (!this.getContainer().getDatabaseType().equalsIgnoreCase("oracle")) {
            recordBuilder.withBoolean("t_boolean", true);
        }

        if (!this.getContainer().getDatabaseType().equalsIgnoreCase("redshift")) {
            recordBuilder.withBytes("t_bytes", "some data in bytes".getBytes(StandardCharsets.UTF_8));
        }

        records.add(recordBuilder.build());
    }

    @Test
    @DisplayName("Create table - Single primary key")
    void createTable(final TestInfo testInfo) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        // final DataSource dataSource = this.getDataSource();
        final JdbcConnection dataStore = newConnection();

        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(dataStore, getI18nMessage()).createTableIfNotExist(connection, testTable, asList("id"),
                        RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
            }
        }
    }

    @Test
    @DisplayName("Create table - Combined primary key")
    void createTableWithCombinedPrimaryKeys(final TestInfo testInfo) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection dataStore = newConnection();

        // final DataSource dataSource = this.getDataSource();
        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {

            try (final Connection connection = dataSource.getConnection()) {
                PlatformFactory.get(dataStore, getI18nMessage()).createTableIfNotExist(connection, testTable,
                        asList("id", "email"), RedshiftSortStrategy.COMPOUND, emptyList(), DistributionStrategy.KEYS, emptyList(),
                        -1, records);
            }
        }
    }

    @Test
    @DisplayName("Create table - existing table")
    void createExistingTable(final TestInfo testInfo) throws SQLException {
        final String testTable = getTestTableName(testInfo);
        final JdbcConnection dataStore = newConnection();
        // final DataSource dataSource = this.getDataSource();
        try (final JdbcService.JdbcDatasource dataSource = getJdbcService().createDataSource(dataStore)) {
            try (final Connection connection = dataSource.getConnection()) {
                Platform platform = PlatformFactory.get(dataStore, getI18nMessage());
                platform.createTableIfNotExist(connection, testTable, asList("id", "email"), RedshiftSortStrategy.COMPOUND,
                        emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
                // recreate the table should not fail
                platform.createTableIfNotExist(connection, testTable, asList("id", "email"), RedshiftSortStrategy.COMPOUND,
                        emptyList(), DistributionStrategy.KEYS, emptyList(), -1, records);
            }
        }
    }

    /*
     * @WithComponents("org.talend.components.jdbc")
     * public static class DerbyPlatformTest extends PlatformTests {
     * 
     * @Override
     * public JdbcTestContainer buildContainer() {
     * return new DerbyTestContainer();
     * }
     * }
     */

    /*
     * @WithComponents("org.talend.components.jdbc")
     * public static class SnowflakePlatformTest extends PlatformTests {
     * 
     * @Override
     * public JdbcTestContainer buildContainer() {
     * return new SnowflakeTestContainer();
     * }
     * }
     */
}
