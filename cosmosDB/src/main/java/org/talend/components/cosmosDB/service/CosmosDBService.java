/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.cosmosDB.service;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RetryOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.cosmosDB.dataset.QueryDataset;
import org.talend.components.cosmosDB.datastore.CosmosDBDataStore;
import org.talend.components.cosmosDB.input.CosmosDBInput;
import org.talend.components.cosmosDB.input.CosmosDBInputConfiguration;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;

@Version(1)
@Slf4j
@Service
public class CosmosDBService {

    @Service
    private I18nMessage i18n;

    @Service
    private RecordBuilderFactory builderFactory;

    public static final String ACTION_SUGGESTION_TABLE_COLUMNS_NAMES = "ACTION_SUGGESTION_TABLE_COLUMNS_NAMES";

    /*
     * Create a document client from specified configuration.
     */
    public DocumentClient documentClientFrom(CosmosDBDataStore datastore) {

        ConnectionPolicy policy = new ConnectionPolicy();
        RetryOptions retryOptions = new RetryOptions();
        retryOptions.setMaxRetryAttemptsOnThrottledRequests(0);
        policy.setRetryOptions(retryOptions);

        policy.setConnectionMode(ConnectionMode.valueOf(datastore.getConnectionMode().name()));
        policy.setMaxPoolSize(datastore.getMaxConnectionPoolSize());

        return new DocumentClient(datastore.getServiceEndpoint(), datastore.getPrimaryKey(), policy,
                ConsistencyLevel.valueOf(datastore.getConsistencyLevel().name()));
    }

    @HealthCheck("healthCheck")
    public HealthCheckStatus healthCheck(@Option("configuration.dataset.connection") final CosmosDBDataStore datastore) {
        String databaseID = datastore.getDatabaseID();
        if (StringUtils.isEmpty(databaseID)) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.vacantDBID());
        }
        try (DocumentClient client = documentClientFrom(datastore)) {
            String databaseLink = String.format("/dbs/%s", databaseID);
            client.readDatabase(databaseLink, null);
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.connectionSuccess());
        } catch (DocumentClientException de) {
            // If the database does not exist, create a new database
            if (de.getStatusCode() == 404) {
                return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.databaseNotExist(databaseID));
            } else {
                return new HealthCheckStatus(HealthCheckStatus.Status.KO, de.getLocalizedMessage());
            }
        } catch (Exception exception) {
            String message = "";
            if (exception.getCause() instanceof RuntimeException && exception.getCause().getCause() instanceof TimeoutException) {
                message = i18n.destinationUnreachable();
            } else {
                message = i18n.connectionKODetailed(exception.getMessage());
            }
            log.error(message, exception);
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, message);
        }
    }

    @DiscoverSchema("discover")
    public Schema addColumns(@Option("dataset") final QueryDataset dataset) {
        CosmosDBInputConfiguration configuration = new CosmosDBInputConfiguration();
        configuration.setDataset(dataset);
        CosmosDBInput cosmosDBInput = new CosmosDBInput(configuration, this, builderFactory, i18n);
        cosmosDBInput.init();
        Record record = cosmosDBInput.next();
        cosmosDBInput.release();
        if (record == null) {
            throw new IllegalArgumentException(i18n.noResultFetched());
        }
        return record.getSchema();
    }

    @Suggestions(ACTION_SUGGESTION_TABLE_COLUMNS_NAMES)
    public SuggestionValues getTableColumns(@Option("schema") final List<String> schema) {
        if (schema.size() > 0) {
            return new SuggestionValues(true, schema.stream().map(columnName -> new SuggestionValues.Item(columnName, columnName))
                    .collect(Collectors.toList()));
        }
        return new SuggestionValues(false, emptyList());
    }
}