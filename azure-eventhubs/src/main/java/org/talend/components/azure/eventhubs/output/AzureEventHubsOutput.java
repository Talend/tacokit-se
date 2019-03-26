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
import java.nio.charset.Charset;
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

import lombok.extern.slf4j.Slf4j;

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

    private EventHubClient ehClient;

    private EventHubClient sender;

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
                .setEndpoint(new URI(configuration.getDataset().getDatastore().getEndpoint()));
        connStr.setSasKeyName(configuration.getDataset().getDatastore().getSasKeyName());
        connStr.setSasKey(configuration.getDataset().getDatastore().getSasKey());
        connStr.setEventHubName(configuration.getDataset().getEventHubName());
        // log.info("init client...");
        ehClient = EventHubClient.createSync(connStr.toString(), executorService);
        sender = EventHubClient.createSync(connStr.toString(), executorService);
        jsonb = JsonbBuilder.create();

    }

    @AfterGroup
    public void afterGroup() {

        try {
            BatchOptions options = new BatchOptions();
            if (AzureEventHubsOutputConfiguration.PartitionType.COLUMN.equals(configuration.getPartitionType())) {
                options.partitionKey = configuration.getKeyColumn();
            }
            final EventDataBatch events = sender.createBatch(options);
            EventData sendEvent;
            for (Record record : records) {
                byte[] payloadBytes = recordToCsvByteArray(record);
                events.tryAdd(EventData.create(payloadBytes));
            }
            sender.sendSync(events);
        } catch (final Exception e) {
            throw new IllegalStateException(e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        try {
            sender.closeSync();
            executorService.shutdown();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private byte[] recordToCsvByteArray(Record record) {
        // TODO make delimited configurable
        StringBuilder sb = new StringBuilder();
        Schema schema = record.getSchema();
        for (Schema.Entry field : schema.getEntries()) {
            if (sb.length() != 0) {
                sb.append(";");
            }
            sb.append(record.getString(field.getName()));
        }
        byte[] bytes = sb.toString().getBytes(Charset.forName("UTF-8"));
        sb.setLength(0);
        return bytes;
    }
}