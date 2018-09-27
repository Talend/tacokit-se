package org.talend.components.jdbc.service;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toList;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.datastore.BasicDatastore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class ActionService {

    public static final String ACTION_LIST_SUPPORTED_DB = "ACTION_LIST_SUPPORTED_DB";

    public static final String ACTION_BASIC_HEALTH_CHECK = "ACTION_BASIC_HEALTH_CHECK";

    private static List<String> SUPPORTED_TYPES = Arrays.asList("TABLE", "VIEW", "SYNONYM");

    @Service
    private JdbcService jdbcDriversService;

    @Service
    private I18nMessage i18n;

    @Configuration("jdbc")
    private Supplier<JdbcConfiguration> jdbcConfiguration;

    @DynamicValues(ACTION_LIST_SUPPORTED_DB)
    public Values loadSupportedDataBaseTypes() {
        return new Values(jdbcConfiguration.get().getDrivers().stream()
                .map(driver -> new Values.Item(driver.getId(), driver.getId())).collect(toList()));
    }

    @HealthCheck(ACTION_BASIC_HEALTH_CHECK)
    public HealthCheckStatus validateBasicDatastore(@Option final BasicDatastore datastore) {
        try {
            this.jdbcDriversService.connection(datastore);
        } catch (final Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }

        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.successConnection());
    }

    @Suggestions("tables.list")
    public SuggestionValues getTableFromDatabase(@Option final BasicDatastore datastore) {
        final Collection<SuggestionValues.Item> items = new HashSet<>();
        try (Connection conn = jdbcDriversService.connection(datastore)) {
            DatabaseMetaData dbMetaData = conn.getMetaData();
            Set<String> tableTypes = getAvailableTableTypes(dbMetaData);
            final String schema = (datastore.getJdbcUrl().contains("oracle") && datastore.getUserId() != null)
                    ? datastore.getUserId().toUpperCase()
                    : null;
            try (ResultSet tables = dbMetaData.getTables(null, schema, null, tableTypes.toArray(new String[0]))) {
                while (tables.next()) {
                    ofNullable(ofNullable(tables.getString("TABLE_NAME")).orElseGet(() -> {
                        try {
                            return tables.getString("SYNONYM_NAME");
                        } catch (SQLException e) {
                            // no-op
                            return null;
                        }
                    })).ifPresent(t -> items.add(new SuggestionValues.Item(t, t)));

                }
            }
        } catch (final Exception unexpected) { // catch all exceptions for this ui action to return empty list
            log.error("can't get table suggestions list from this database", unexpected);
        }
        return new SuggestionValues(true, items);
    }

    private Set<String> getAvailableTableTypes(DatabaseMetaData dbMetaData) throws SQLException {
        Set<String> result = new HashSet<String>();
        try (ResultSet tables = dbMetaData.getTableTypes()) {
            while (tables.next()) {
                ofNullable(tables.getString("TABLE_TYPE")).map(String::trim)
                        .map(t -> ("BASE TABLE".equalsIgnoreCase(t)) ? "TABLE" : t).filter(t -> SUPPORTED_TYPES.contains(t))
                        .ifPresent(result::add);
            }
        }
        return result;
    }

}
