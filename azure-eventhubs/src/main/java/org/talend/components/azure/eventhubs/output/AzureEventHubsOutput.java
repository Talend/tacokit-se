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
 *
 */

package org.talend.components.azure.eventhubs.output;

import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PreDestroy;
import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;

import org.talend.components.azure.eventhubs.service.Messages;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.configuration.LocalConfiguration;

import com.microsoft.azure.eventhubs.BatchOptions;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventDataBatch;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.PartitionSender;

import lombok.extern.slf4j.Slf4j;

import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_CHARSET;

@Slf4j
@Version
@Icon(Icon.IconType.DEFAULT)
@Processor(name = "AzureEventHubsOutput")
@Documentation("AzureEventHubs output")
public class AzureEventHubsOutput implements Serializable {

    private final AzureEventHubsOutputConfiguration configuration;

    private final LocalConfiguration localConfiguration;

    private transient List<Record> records;

    private Messages messages;

    private boolean init;

    private ScheduledExecutorService executorService;

    private EventHubClient eventHubClient;

    private PartitionSender partitionSender;

    private Jsonb jsonb;

    public AzureEventHubsOutput(@Option("configuration") final AzureEventHubsOutputConfiguration outputConfig,
            final LocalConfiguration localConfiguration, final Messages messages) {
        this.configuration = outputConfig;
        this.localConfiguration = localConfiguration;
        this.messages = messages;
    }

    @BeforeGroup
    public void beforeGroup() {
        this.records = new ArrayList<>();
    }

    @ElementListener
    public void elementListener(@Input final Record record) throws URISyntaxException, IOException, EventHubException {
        if (!init) {
            // prevent creating db connection if no records
            // it's mostly useful for streaming scenario
            lazyInit();
        }
        records.add(record);
    }

    private void lazyInit() throws URISyntaxException, IOException, EventHubException {
        this.init = true;
        executorService = Executors.newScheduledThreadPool(1);
        final ConnectionStringBuilder connStr = new ConnectionStringBuilder()//
                .setEndpoint(new URI(configuration.getDataset().getConnection().getEndpoint()));
        connStr.setSasKeyName(configuration.getDataset().getConnection().getSasKeyName());
        connStr.setSasKey(configuration.getDataset().getConnection().getSasKey());
        connStr.setEventHubName(configuration.getDataset().getEventHubName());

        eventHubClient = EventHubClient.createSync(connStr.toString(), executorService);
        if (AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID.equals(configuration.getPartitionType())) {
            partitionSender = eventHubClient.createPartitionSenderSync(configuration.getPartitionId());
        }
        jsonb = JsonbBuilder.create();

    }

    @AfterGroup
    public void afterGroup() {

        try {
            BatchOptions options = new BatchOptions();
            if (AzureEventHubsOutputConfiguration.PartitionType.COLUMN.equals(configuration.getPartitionType())) {
                options.partitionKey = configuration.getKeyColumn();
            }
            final EventDataBatch events = eventHubClient.createBatch(options);
            for (Record record : records) {
                byte[] payloadBytes = recordToCsvByteArray(record);
                events.tryAdd(EventData.create(payloadBytes));
            }
            if (AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID.equals(configuration.getPartitionType())) {
                partitionSender.sendSync(events);
            } else {
                eventHubClient.sendSync(events);
            }
        } catch (final Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            if (partitionSender != null) {
                partitionSender.closeSync();
            }
            eventHubClient.closeSync();
            executorService.shutdown();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private byte[] recordToCsvByteArray(Record record) throws IOException {
        // TODO make delimited configurable
        StringBuilder sb = new StringBuilder();
        Schema schema = record.getSchema();
        boolean isFirstColumn = true;
        for (Schema.Entry field : schema.getEntries()) {
            if (!isFirstColumn) {
                sb.append(";");
            } else {
                isFirstColumn = false;
            }
            sb.append(getStringValue(record, field));

        }
        byte[] bytes = sb.toString().getBytes(DEFAULT_CHARSET);
        sb.setLength(0);
        return bytes;
    }

    private String getStringValue(Record record, Schema.Entry field) throws IOException {
        Object value = null;
        switch (field.getType()) {
        case STRING:
            value = record.getString(field.getName());
            break;
        case BOOLEAN:
            if (record.getOptionalBoolean(field.getName()).isPresent()) {
                value = record.getBoolean(field.getName());
            }
            break;
        case DOUBLE:
            if (record.getOptionalDouble(field.getName()).isPresent()) {
                value = record.getDouble(field.getName());
            }
            break;
        case FLOAT:
            if (record.getOptionalFloat(field.getName()).isPresent()) {
                value = record.getFloat(field.getName());
            }
            break;
        case LONG:
            if (record.getOptionalLong(field.getName()).isPresent()) {
                value = record.getLong(field.getName());
            }
            break;
        case INT:
            if (record.getOptionalInt(field.getName()).isPresent()) {
                value = record.getInt(field.getName());
            }
            break;
        case DATETIME:
            value = record.getDateTime(field.getName());
            break;
        case BYTES:
            if (record.getOptionalBytes(field.getName()).isPresent()) {
                value = new String(record.getBytes(field.getName()), DEFAULT_CHARSET);
            }
            break;
        default:
            throw new IllegalStateException(messages.errorUnsupportedType(field.getType().name(), field.getName()));
        }
        return value == null ? "" : String.valueOf(value);

    }
}