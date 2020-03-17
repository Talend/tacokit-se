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
package org.talend.components.cosmosDB.service;

import com.microsoft.azure.documentdb.ConnectionMode;
import com.microsoft.azure.documentdb.ConnectionPolicy;
import com.microsoft.azure.documentdb.ConsistencyLevel;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.RetryOptions;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONArray;
import org.json.JSONObject;
import org.talend.components.cosmosDB.dataset.CosmosDBDataset;
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

import java.util.Collection;
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
        try (DocumentClient client = documentClientFrom(datastore)) {
            String databaseLink = String.format("/dbs/%s", datastore.getDatabaseID());
            client.readDatabase(databaseLink, null);
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, "Connection OK");
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
    public Schema addColumns(@Option("dataset") final CosmosDBDataset dataSet) {
        CosmosDBInputConfiguration configuration = new CosmosDBInputConfiguration();
        configuration.setDataset(dataSet);
        CosmosDBInput cosmosDBInput = new CosmosDBInput(configuration, this, builderFactory, i18n);
        cosmosDBInput.init();
        Record record = cosmosDBInput.next();
        cosmosDBInput.release();
        if (record == null) {
            throw new IllegalArgumentException("No result fetched from source collection");
        }
        return record.getSchema();
    }

    @Suggestions(ACTION_SUGGESTION_TABLE_COLUMNS_NAMES)
    public SuggestionValues getTableColumns(@Option final List<String> schema) {
        if (schema.size() > 0) {
            return new SuggestionValues(true, schema.stream().map(columnName -> new SuggestionValues.Item(columnName, columnName))
                    .collect(Collectors.toList()));
        }
        return new SuggestionValues(false, emptyList());
    }

    public JSONObject record2JSONObject(Record record) {
        if (record == null) {
            return null;
        }
        JSONObject json = new JSONObject();

        for (Schema.Entry entry : record.getSchema().getEntries()) {
            final String fieldName = entry.getName();
            Object val = record.get(Object.class, fieldName);
            log.debug("[convertRecordToJsonObject] entry: {}; type: {}; value: {}.", fieldName, entry.getType(), val);
            if (null == val) {
                json.put(fieldName, JSONObject.NULL);
            } else {
                this.addField(json, record, entry);
            }
        }
        return json;
    }

    private JSONArray toJsonArray(Collection<Object> objects) {
        JSONArray array = new JSONArray();
        for (Object obj : objects) {
            if (obj instanceof Collection) {
                JSONArray subArray = toJsonArray((Collection) obj);
                array.put(subArray);
            } else if (obj instanceof String) {
                array.put((String) obj);
            } else if (obj instanceof Record) {
                JSONObject subObject = record2JSONObject((Record) obj);
                array.put(subObject);
            } else if (obj instanceof Integer) {
                array.put((Integer) obj);
            } else if (obj instanceof Long) {
                array.put((Long) obj);
            } else if (obj instanceof Double) {
                array.put((Double) obj);
            } else if (obj instanceof Boolean) {
                array.put((Boolean) obj);
            }
        }
        return array;
    }

    private void addField(JSONObject json, Record record, Schema.Entry entry) {
        final String fieldName = entry.getName();
        switch (entry.getType()) {
        case RECORD:
            final Record subRecord = record.getRecord(fieldName);
            json.put(fieldName, record2JSONObject(subRecord));
            break;
        case ARRAY:
            final Collection<Object> array = record.getArray(Object.class, fieldName);
            final JSONArray jarray = toJsonArray(array);
            json.put(fieldName, jarray);
            break;
        case STRING:
            json.put(fieldName, record.getString(fieldName));
            break;
        case BYTES:
            json.put(fieldName, new String(record.getBytes(fieldName)));
            break;
        case INT:
            json.put(fieldName, record.getInt(fieldName));
            break;
        case LONG:
            json.put(fieldName, record.getLong(fieldName));
            break;
        case FLOAT:
            json.put(fieldName, record.getFloat(fieldName));
            break;
        case DOUBLE:
            json.put(fieldName, record.getDouble(fieldName));
            break;
        case BOOLEAN:
            json.put(fieldName, record.getBoolean(fieldName));
            break;
        case DATETIME:
            json.put(fieldName, record.getDateTime(fieldName).toString());
            break;
        }
    }

}