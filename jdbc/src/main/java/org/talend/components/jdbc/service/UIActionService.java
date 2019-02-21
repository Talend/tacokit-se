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
package org.talend.components.jdbc.service;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.JdbcConfiguration;
import org.talend.components.jdbc.dataset.TableNameDataset;
import org.talend.components.jdbc.datastore.JdbcConnection;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import java.sql.*;
import java.util.*;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Comparator.comparingInt;
import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

@Slf4j
@Service
public class UIActionService {

    public static final String ACTION_LIST_SUPPORTED_DB = "ACTION_LIST_SUPPORTED_DB";

    public static final String ACTION_LIST_HANDLERS_DB = "ACTION_LIST_HANDLERS_DB";

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    public static final String ACTION_VALIDATION_READONLY_QUERY = "ACTION_VALIDATION_READONLY_QUERY";

    public static final String ACTION_SUGGESTION_TABLE_NAMES = "ACTION_SUGGESTION_TABLE_NAMES";

    public static final String ACTION_SUGGESTION_TABLE_COLUMNS_NAMES = "ACTION_SUGGESTION_TABLE_COLUMNS_NAMES";

    @Service
    private JdbcService jdbcService;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @DynamicValues(ACTION_LIST_SUPPORTED_DB)
    public Values loadSupportedDataBaseTypes() {
        return new Values(jdbcConfiguration.get().getDrivers().stream().filter(d -> jdbcService.driverNotDisabled(d))
                .sorted(comparingInt(JdbcConfiguration.Driver::getOrder))
                .map(driver -> new Values.Item(driver.getId(), driver.getDisplayName())).collect(toList()));
    }

    @Suggestions(ACTION_LIST_HANDLERS_DB)
    public SuggestionValues getHandlersDataBaseTypes(@Option final String dbType) {
        List<JdbcConfiguration.Driver> drivers = jdbcConfiguration.get().getDrivers().stream()
                .filter(d -> jdbcService.driverNotDisabled(d)).collect(toList());
        return new SuggestionValues(false, drivers.stream().filter(d -> jdbcService.driverNotDisabled(d))
                .filter(db -> db.getId().equals(dbType) && !db.getHandlers().isEmpty()).flatMap(db -> db.getHandlers().stream())
                .flatMap(handler -> drivers.stream().filter(d -> d.getId().equals(handler))).distinct()
                .sorted(comparingInt(JdbcConfiguration.Driver::getOrder))
                .map(driver -> new SuggestionValues.Item(driver.getId(), driver.getDisplayName())).collect(toList()));
    }

    @HealthCheck(ACTION_BASIC_HEALTH_CHECK)
    public HealthCheckStatus validateBasicDataStore(@Option final JdbcConnection datastore) {
        try (JdbcService.JdbcDatasource dataSource = this.jdbcService.createDataSource(datastore)) {
            try (final Connection ignored = dataSource.getConnection()) {
                // no-op
            } catch (final Exception e) {
                return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
            }

            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
        }
    }

    @AsyncValidation(ACTION_VALIDATION_READONLY_QUERY)
    public ValidationResult validateReadOnlySQLQuery(final String query) {
        if (jdbcService.isNotReadOnlySQLQuery(query)) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.errorUnauthorizedQuery());
        }
        return new ValidationResult(ValidationResult.Status.OK, "the query is valid");
    }

    @Suggestions(ACTION_SUGGESTION_TABLE_COLUMNS_NAMES)
    public SuggestionValues getTableColumns(@Option final TableNameDataset dataset) {
        try (Connection conn = jdbcService.createDataSource(dataset.getConnection()).getConnection()) {
            try (final Statement statement = conn.createStatement()) {
                statement.setMaxRows(1);
                try (final ResultSet result = statement.executeQuery(dataset.getQuery())) {
                    return new SuggestionValues(true,
                            IntStream.rangeClosed(1, result.getMetaData().getColumnCount()).mapToObj(i -> {
                                try {
                                    return result.getMetaData().getColumnName(i);
                                } catch (SQLException e) {
                                    e.printStackTrace();
                                }

                                return null;
                            }).filter(Objects::nonNull).map(columnName -> new SuggestionValues.Item(columnName, columnName))
                                    .collect(toSet()));
                }
            }
        } catch (final Exception unexpected) {
            // catch all exceptions for this ui action to return empty list
            log.error(i18n.errorCantLoadTableSuggestions(), unexpected);
        }

        return new SuggestionValues(false, emptyList());
    }

    @Suggestions(ACTION_SUGGESTION_TABLE_NAMES)
    public SuggestionValues getTableFromDatabase(@Option final JdbcConnection datastore) {
        final Collection<SuggestionValues.Item> items = new HashSet<>();
        try (Connection connection = jdbcService.createDataSource(datastore).getConnection()) {
            final DatabaseMetaData dbMetaData = connection.getMetaData();
            try (ResultSet tables = dbMetaData.getTables(connection.getCatalog(), connection.getSchema(), null,
                    getAvailableTableTypes(dbMetaData).toArray(new String[0]))) {
                while (tables.next()) {
                    ofNullable(ofNullable(tables.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return tables.getString("SYNONYM_NAME");
                        } catch (final SQLException e) {
                            return null;
                        }
                    })).ifPresent(t -> items.add(new SuggestionValues.Item(t, t)));
                }
            }
        } catch (final Exception unexpected) { // catch all exceptions for this ui action to return empty list
            log.error(i18n.errorCantLoadTableSuggestions(), unexpected);
        }
        return new SuggestionValues(true, items);
    }

    private Set<String> getAvailableTableTypes(DatabaseMetaData dbMetaData) throws SQLException {
        Set<String> result = new HashSet<>();
        try (ResultSet tables = dbMetaData.getTableTypes()) {
            while (tables.next()) {
                ofNullable(tables.getString("TABLE_TYPE")).map(String::trim)
                        .map(t -> ("BASE TABLE".equalsIgnoreCase(t)) ? "TABLE" : t)
                        .filter(t -> jdbcConfiguration.get().getSupportedTableTypes().contains(t)).ifPresent(result::add);
            }
        }
        return result;
    }

}
