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
package org.talend.components.azure.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.talend.components.azure.common.AzureConnection;
import org.talend.components.azure.common.AzureTableConnection;
import org.talend.components.azure.table.output.OutputProperties;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.OperationContext;
import com.microsoft.azure.storage.table.CloudTableClient;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableQuery;

@Service
public class AzureComponentServices {

    public static final String COLUMN_NAMES = "getColumnNames";

    public static final String TEST_CONNECTION = "testConnection";

    public static final String GET_TABLE_NAMES = "getTableNames";

    public static final String GUESS_SCHEMA = "guessSchema";

    public static final String SUPPORTED_ACTION_ON_TABLE = "supportedActionOnTable";

    @Service
    RecordBuilderFactory factory;

    @Service
    MessageService i18nService;

    @Service
    AzureConnectionService connectionService;

    @HealthCheck(TEST_CONNECTION)
    public HealthCheckStatus testConnection(@Option AzureConnection azureConnection) {
        final int maxTables = 1;
        try {
            CloudStorageAccount cloudStorageAccount = connectionService.createStorageAccount(azureConnection);
            CloudTableClient tableClient = connectionService.createCloudTableClient(cloudStorageAccount,
                    AzureConnectionService.DEFAULT_RETRY_POLICY);
            // will throw an exception if not authorized or account not exist
            tableClient.listTablesSegmented(null, maxTables, null, null, AzureConnectionService.getTalendOperationContext());
        } catch (Exception e) {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, e.getMessage());
        }
        return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18nService.connected());
    }

    @Suggestions(GET_TABLE_NAMES)
    public SuggestionValues getTableNames(@Option AzureConnection azureConnection) {
        List<SuggestionValues.Item> tableNames = new ArrayList<>();
        try {
            CloudStorageAccount storageAccount = connectionService.createStorageAccount(azureConnection);
            final OperationContext operationContext = AzureConnectionService.getTalendOperationContext();
            for (String tableName : storageAccount.createCloudTableClient().listTables(null, null, operationContext)) {
                tableNames.add(new SuggestionValues.Item(tableName, tableName));
            }

        } catch (Exception e) {
            throw new RuntimeException(i18nService.errorRetrieveTables(), e);
        }

        return new SuggestionValues(true, tableNames);
    }

    @Suggestions(value = COLUMN_NAMES)
    public SuggestionValues getColumnNames(@Option List<String> schema) {
        List<SuggestionValues.Item> suggestionItemList = schema.stream().map(s -> new SuggestionValues.Item(s, s))
                .collect(Collectors.toList());

        return new SuggestionValues(false, suggestionItemList);
    }

    @DiscoverSchema(GUESS_SCHEMA)
    public Schema guessSchema(@Option final AzureTableConnection configuration) {
        final Schema.Entry.Builder entryBuilder = factory.newEntryBuilder();
        final Schema.Builder schemaBuilder = factory.newSchemaBuilder(Schema.Type.RECORD);
        // add 3 default columns
        schemaBuilder.withEntry(entryBuilder.withName("PartitionKey").withType(Schema.Type.STRING).build())
                .withEntry(entryBuilder.withName("RowKey").withType(Schema.Type.STRING).build())
                .withEntry(entryBuilder.withName("Timestamp").withType(Schema.Type.DATETIME).build());
        String tableName = configuration.getTableName();
        try {
            AzureConnection connection = configuration.getConnection();
            TableQuery<DynamicTableEntity> partitionQuery = TableQuery.from(DynamicTableEntity.class).take(1);
            CloudStorageAccount account = connectionService.createStorageAccount(connection);
            Iterator<DynamicTableEntity> entitiesIterator = connectionService.executeQuery(account, tableName, partitionQuery)
                    .iterator();
            if (entitiesIterator.hasNext()) {
                DynamicTableEntity result = entitiesIterator.next();
                for (Map.Entry<String, EntityProperty> f : result.getProperties().entrySet()) {
                    schemaBuilder.withEntry(entryBuilder.withName(f.getKey())
                            .withType(getAppropriateType(f.getValue().getEdmType())).withNullable(true).build());
                }
            }

        } catch (Exception e) {
            throw new RuntimeException(i18nService.errorRetrieveSchema(), e);
        }
        return schemaBuilder.build();
    }

    private Schema.Type getAppropriateType(EdmType edmType) {
        switch (edmType) {
        case BOOLEAN:
            return Schema.Type.BOOLEAN;
        case BINARY:
            return Schema.Type.BYTES;
        case BYTE:
        case SBYTE:
        case INT16:
        case INT32:
            return Schema.Type.INT;
        case GUID:
        case INT64:
            return Schema.Type.LONG;
        case SINGLE:
            return Schema.Type.FLOAT;
        case DECIMAL:
        case DOUBLE:
            return Schema.Type.DOUBLE;
        case DATE_TIME:
        case DATE_TIME_OFFSET:
            return Schema.Type.DATETIME;
        default:
            return Schema.Type.STRING;
        }
    }

    /**
     * Before writer init would be implemented in framework, we can't fulfil all actions on table implementation in cloud
     * Therefore it's needed to show only DEFAULT and CREATE_IF_NOT_EXIST options
     * 
     * @return supported actions on table
     */
    @DynamicValues(SUPPORTED_ACTION_ON_TABLE)
    public Values loadSupportedActionOnTables() {
        Collection<Values.Item> item = new ArrayList<>();
        item.add(new Values.Item(OutputProperties.ActionOnTable.DEFAULT.name(), "Default"));
        item.add(new Values.Item(OutputProperties.ActionOnTable.CREATE_IF_NOT_EXIST.name(), "Create table if not exist"));
        return new Values(item);
    }
}
