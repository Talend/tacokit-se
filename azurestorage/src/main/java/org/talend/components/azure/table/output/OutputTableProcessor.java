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
package org.talend.components.azure.table.output;

import java.io.IOException;
import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.azure.common.NameMapping;
import org.talend.components.azure.service.AzureConnectionService;
import org.talend.components.azure.service.AzureTableUtils;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.CloudTable;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableBatchOperation;
import com.microsoft.azure.storage.table.TableOperation;
import com.microsoft.azure.storage.table.TableResult;

@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = Icon.IconType.CUSTOM, custom = "outputTable")
@Processor(name = "OutputTable")
@Documentation("Azure Output Table Component")
public class OutputTableProcessor implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutputTableProcessor.class);

    private final OutputProperties configuration;

    private final AzureConnectionService service;

    private CloudStorageAccount connection;

    private List<Record> recordToEnqueue = new ArrayList<>();

    private List<TableOperation> batchOperations = new ArrayList<>();

    private int batchOperationsCount;

    private String latestPartitionKey;

    private static final int MAX_RECORDS_TO_ENQUEUE = 250;

    private MessageService i18nService;

    private int DEFAULT_BATCH_SIZE = 100;

    public OutputTableProcessor(@Option("configuration") final OutputProperties configuration,
            final AzureConnectionService service, final MessageService i18nService) {
        this.configuration = configuration;
        this.service = service;
        this.i18nService = i18nService;
    }

    @PostConstruct
    public void init() throws Exception {
        try {
            connection = service.createStorageAccount(configuration.getAzureConnection().getConnection());
            LOGGER.debug(i18nService.connected());
        } catch (URISyntaxException e) {
            throw new RuntimeException(i18nService.connectionError(), e);
        }

        handleActionOnTable();
    }

    @ElementListener
    public void onNext(@Input final Record incomingRecord) throws Exception {
        if (incomingRecord == null) {
            return;
        }
        if (configuration.isProcessInBatch()) {
            DynamicTableEntity entity = createDynamicEntityFromInputRecord(incomingRecord);
            addOperationToBatch(entity);
        } else {
            recordToEnqueue.add(incomingRecord);
            if (recordToEnqueue.size() >= MAX_RECORDS_TO_ENQUEUE) {
                processParallelRecords();
            }
        }
    }

    @PreDestroy
    public void release() {
        if (batchOperationsCount > 0) {
            LOGGER.debug("Executing last batch");
            processBatch();
        }

        if (recordToEnqueue.size() > 0) {
            processParallelRecords();
        }
    }

    private void processParallelRecords() {
        recordToEnqueue.parallelStream().forEach(record -> {
            try {
                DynamicTableEntity entity = createDynamicEntityFromInputRecord(record);
                executeOperation(getTableOperation(entity));
            } catch (StorageException e) {
                LOGGER.error(i18nService.errorExecutingOperation(), e);
                if (configuration.isDieOnError()) {
                    throw new RuntimeException(e);
                }

            } catch (URISyntaxException e) {
                throw new RuntimeException(e); // connection problem so next operation will also fail, we stop the process
            }
        });
        recordToEnqueue.clear();
    }

    private void handleActionOnTable() throws IOException, StorageException, URISyntaxException {
        String tableName = configuration.getAzureConnection().getTableName();
        switch (OutputProperties.ActionOnTable.valueOf(configuration.getActionOnTable())) {
        case CREATE:
            service.createTable(connection, tableName);
            break;
        case CREATE_IF_NOT_EXIST:
            service.createTableIfNotExists(connection, tableName);
            break;
        case DROP_AND_CREATE:
            service.deleteTableAndCreate(connection, tableName);
            break;
        case DROP_IF_EXIST_CREATE:
            service.deleteTableIfExistsAndCreate(connection, tableName);
            break;
        case DEFAULT:
        default:
        }

    }

    private ArrayList<TableResult> executeOperation(TableBatchOperation batchOpe) throws URISyntaxException, StorageException {

        CloudTable cloudTable = service.createTableClient(connection, configuration.getAzureConnection().getTableName());
        return cloudTable.execute(batchOpe, null, AzureConnectionService.getTalendOperationContext());
    }

    private void processBatch() {
        TableBatchOperation batch = new TableBatchOperation();
        batch.addAll(batchOperations);

        try {
            executeOperation(batch);
        } catch (StorageException e) {
            LOGGER.error(i18nService.errorExecutingBatch(), e);
            if (configuration.isDieOnError()) {
                throw new RuntimeException(e);
            }
        } catch (URISyntaxException e) {
            throw new RuntimeException(e); // connection problem so next operation will also fail, we stop the process
        }
        resetBatch();
    }

    private void resetBatch() {
        batchOperations.clear();
        batchOperationsCount = 0;
        latestPartitionKey = "";
    }

    private DynamicTableEntity createDynamicEntityFromInputRecord(Record incomingRecord) {
        DynamicTableEntity entity = new DynamicTableEntity();
        HashMap<String, EntityProperty> entityProps = new HashMap<>();
        for (Schema.Entry field : incomingRecord.getSchema().getEntries()) {
            if (incomingRecord.get(Object.class, field.getName()) == null) {
                continue; // record value may be null, No need to set the property in azure in this case
            }
            if (field.getType() == Schema.Type.RECORD) {
                continue; // NOOP needed for nested Records for now
            }
            String columnName = field.getName();
            String mappedName = getMappedNameIfNecessary(columnName);

            if (columnName.equals(configuration.getPartitionName())) {
                entity.setPartitionKey(incomingRecord.getString(columnName));
            } else if (columnName.equals(configuration.getRowKey())) {
                entity.setRowKey(incomingRecord.getString(columnName));
            } else if (!mappedName.equals(AzureTableUtils.TABLE_TIMESTAMP)) {
                EntityProperty propertyValue = createEntityProperty(incomingRecord, field, columnName);
                entityProps.put(mappedName, propertyValue);
            } // nop for timestamp: managed by server
        }
        // Etag is needed for some operations (delete, merge, replace) but we rely only on PK and RK for those ones.
        entity.setEtag("*");
        entity.setProperties(entityProps);
        return entity;
    }

    private EntityProperty createEntityProperty(Record incomingRecord, Schema.Entry entry, String schemaName) {
        EntityProperty propertyValue;
        switch (entry.getType()) {
        case BOOLEAN:
            propertyValue = new EntityProperty(incomingRecord.getBoolean(schemaName));
            break;
        case DOUBLE:
            propertyValue = new EntityProperty(incomingRecord.getDouble(schemaName));
            break;
        case INT:
            propertyValue = new EntityProperty(incomingRecord.getInt(schemaName));
            break;
        case LONG:
            propertyValue = new EntityProperty(incomingRecord.getLong(schemaName));
            break;
        case BYTES:
            propertyValue = new EntityProperty(incomingRecord.getBytes(schemaName));
            break;
        case DATETIME:
            propertyValue = new EntityProperty(Date.from(incomingRecord.getDateTime(schemaName).toInstant()));
            break;
        default: // use string as default type for string and other types...
            propertyValue = new EntityProperty(incomingRecord.getString(schemaName));
        }
        return propertyValue;
    }

    private String getMappedNameIfNecessary(String sName) {
        // FIXME nameMappings should not be null - bug of studio integration plugin
        if (configuration.getNameMappings() != null && configuration.getNameMappings().size() > 0) {
            NameMapping usedNameMapping = configuration.getNameMappings().stream()
                    .filter(nameMapping -> sName.equals(nameMapping.getSchemaColumnName())).findFirst().get();
            return usedNameMapping.getEntityPropertyName();
        }

        return sName;
    }

    private void addOperationToBatch(DynamicTableEntity entity) {
        if (latestPartitionKey == null || latestPartitionKey.isEmpty()) {
            latestPartitionKey = entity.getPartitionKey();
        }
        // we reached the threshold for batch OR changed PartitionKey
        if (batchOperationsCount == DEFAULT_BATCH_SIZE || !entity.getPartitionKey().equals(latestPartitionKey)) {
            processBatch();
        }
        latestPartitionKey = entity.getPartitionKey();
        TableOperation to = getTableOperation(entity);
        batchOperations.add(to);
        batchOperationsCount++;
    }

    private TableResult executeOperation(TableOperation ope) throws URISyntaxException, StorageException {

        CloudTable cloudTable = service.createTableClient(connection, configuration.getAzureConnection().getTableName());
        return cloudTable.execute(ope, null, AzureConnectionService.getTalendOperationContext());
    }

    private TableOperation getTableOperation(DynamicTableEntity entity) {
        switch (configuration.getActionOnData()) {
        case INSERT:
            return TableOperation.insert(entity);
        case INSERT_OR_MERGE:
            return TableOperation.insertOrMerge(entity);
        case INSERT_OR_REPLACE:
            return TableOperation.insertOrReplace(entity);
        case MERGE:
            return TableOperation.merge(entity);
        case REPLACE:
            return TableOperation.replace(entity);
        case DELETE:
            return TableOperation.delete(entity);
        default:
            throw new IllegalArgumentException("Wrong or no action on data was selected");
        }
    }
}