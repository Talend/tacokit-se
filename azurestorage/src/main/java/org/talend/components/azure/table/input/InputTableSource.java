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
package org.talend.components.azure.table.input;

import java.io.Serializable;
import java.net.URISyntaxException;
import java.util.Iterator;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.talend.components.azure.service.AzureConnectionService;
import org.talend.components.azure.service.AzureTableUtils;
import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.table.DynamicTableEntity;
import com.microsoft.azure.storage.table.EntityProperty;
import com.microsoft.azure.storage.table.TableQuery;

@Documentation("Reader for AzureInputTable component")
public class InputTableSource implements Serializable {

    private final InputProperties configuration;

    private final AzureConnectionService service;

    private String filter;

    private CloudStorageAccount connection;

    private Iterator<DynamicTableEntity> recordsIterator;

    private DynamicTableEntity currentEntity;

    private RecordBuilderFactory recordBuilderFactory;

    private MessageService i18nService;

    public InputTableSource(@Option("configuration") final InputProperties configuration, final AzureConnectionService service,
            final RecordBuilderFactory recordBuilderFactory, final MessageService i18nService) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.i18nService = i18nService;
    }

    @PostConstruct
    public void init() {
        try {
            connection = service.createStorageAccount(configuration.getAzureConnection().getConnection());
            if (configuration.isUseFilterExpression()) {
                filter = AzureTableUtils.generateCombinedFilterConditions(configuration);
            }
            executeSelect();
        } catch (URISyntaxException e) {
            throw new RuntimeException(i18nService.connectionError(), e);
        }
    }

    private void executeSelect() {
        try {
            TableQuery<DynamicTableEntity> partitionQuery;
            if (StringUtils.isEmpty(filter)) {
                partitionQuery = TableQuery.from(DynamicTableEntity.class);
            } else {
                partitionQuery = TableQuery.from(DynamicTableEntity.class).where(filter);
            }
            // Using execute will automatically and lazily follow the continuation tokens from page to page of results.
            // So, we bypass the 1000 entities limit.
            Iterable<DynamicTableEntity> entities = service.executeQuery(connection,
                    configuration.getAzureConnection().getTableName(), partitionQuery);
            recordsIterator = entities.iterator();
            if (recordsIterator.hasNext()) {
                currentEntity = recordsIterator.next();
            }
        } catch (URISyntaxException | StorageException e) {
            throw new RuntimeException(i18nService.errorRetrieveData(), e);
        }
    }

    @Producer
    public Record next() {
        Record currentRecord = null;
        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        if (currentEntity != null) {
            builder.withString("PartitionKey", currentEntity.getPartitionKey()).withString("RowKey", currentEntity.getRowKey())
                    .withDateTime("Timestamp", currentEntity.getTimestamp());
            for (Map.Entry<String, EntityProperty> pair : currentEntity.getProperties().entrySet()) {
                String columnName = pair.getKey();
                EntityProperty columnValue = pair.getValue();
                if (configuration.getSchema().contains(columnName)) {
                    addValue(builder, columnName, columnValue);
                }
            }
            currentRecord = builder.build();
            // read record for next iteration
            if (recordsIterator.hasNext()) {
                currentEntity = recordsIterator.next();
            } else {
                currentEntity = null;
            }
        }
        return currentRecord;
    }

    private void addValue(Record.Builder currentRecordBuilder, String columnName, EntityProperty columnValue) {
        switch (columnValue.getEdmType()) {
        case BOOLEAN:
            currentRecordBuilder.withBoolean(columnName, columnValue.getValueAsBoolean());
            break;
        case BINARY:
            currentRecordBuilder.withBytes(columnName, columnValue.getValueAsByteArray());
            break;
        case BYTE:
        case SBYTE:
        case INT16:
        case INT32:
            currentRecordBuilder.withInt(columnName, columnValue.getValueAsInteger());
            break;
        case INT64:
        case GUID:
            currentRecordBuilder.withLong(columnName, columnValue.getValueAsLong());
            break;
        case DECIMAL:
        case SINGLE:
        case DOUBLE:
            currentRecordBuilder.withDouble(columnName, columnValue.getValueAsDouble());
            break;
        case DATE_TIME:
        case DATE_TIME_OFFSET:
            currentRecordBuilder.withDateTime(columnName, columnValue.getValueAsDate());
            break;
        default:
            currentRecordBuilder.withString(columnName, columnValue.getValueAsString());
        }
    }

    @PreDestroy
    public void release() {
        // NOOP
    }
}