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

package org.talend.components.azure.eventhubs.source;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.azure.eventhubs.service.UiActionService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;
import com.microsoft.azure.eventhubs.EventPosition;
import com.microsoft.azure.eventhubs.PartitionReceiver;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Documentation("Source to consume eventhubs messages")
public class AzureEventHubsSource implements Serializable {

    private static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

    private static final String PAYLOAD_COLUMN = "payload";

    private final AzureEventHubsInputConfiguration configuration;

    private final UiActionService service;

    private final RecordBuilderFactory builderFactory;

    private PartitionReceiver receiver;

    private ScheduledExecutorService executorService;

    private Iterator<EventData> receivedEvents;

    private EventHubClient ehClient;

    private long count;

    public AzureEventHubsSource(@Option("configuration") final AzureEventHubsInputConfiguration configuration,
            final UiActionService service, final RecordBuilderFactory builderFactory) {
        this.configuration = configuration;
        this.service = service;
        this.builderFactory = builderFactory;
    }

    @PostConstruct
    public void init() {
        try {
            executorService = Executors.newScheduledThreadPool(8);
            final ConnectionStringBuilder connStr = new ConnectionStringBuilder()//
                    .setEndpoint(new URI(configuration.getDataset().getDatastore().getEndpoint()));
            connStr.setSasKeyName(configuration.getDataset().getDatastore().getSasKeyName());
            connStr.setSasKey(configuration.getDataset().getDatastore().getSasKey());
            connStr.setEventHubName(configuration.getDataset().getEventHubName());
            // log.info("init client...");
            ehClient = EventHubClient.createSync(connStr.toString(), executorService);

            receiver = ehClient.createEpochReceiverSync(configuration.getConsumerGroupName(),
                    configuration.getDataset().getPartitionId(), getPosition(), Integer.MAX_VALUE);
            receiver.setReceiveTimeout(Duration.ofSeconds(configuration.getReceiveTimeout()));
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }

    }

    @Producer
    public Record next() {
        if (configuration.isUseMaxNum() && count >= configuration.getMaxNumReceived()) {
            return null;
        }
        while (true) {
            try {
                if (receivedEvents == null || !receivedEvents.hasNext()) {
                    log.info("fetch messages...");
                    // TODO let it configurable?
                    Iterable<EventData> iterable = receiver.receiveSync(100);
                    if (iterable == null) {
                        if (!configuration.isUseMaxNum()) {
                            return null;
                        } else {
                            continue;
                        }
                    }
                    receivedEvents = iterable.iterator();
                }
                if (receivedEvents.hasNext()) {
                    EventData eventData = receivedEvents.next();
                    if (eventData != null) {
                        Record.Builder recordBuilder = builderFactory.newRecordBuilder();
                        recordBuilder.withString(PAYLOAD_COLUMN, new String(eventData.getBytes(), DEFAULT_CHARSET));
                        // TODO remove this later
                        // log.info(eventData.getSystemProperties().getSequenceNumber() + " --> "
                        // + new String(eventData.getBytes(), DEFAULT_CHARSET));
                        count++;
                        return recordBuilder.build();
                    }
                } else {
                    continue;
                }
            } catch (EventHubException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    @PreDestroy
    public void release() {
        try {
            if (receiver != null) {
                // log.info("release client...");
                receiver.close().thenComposeAsync(aVoid -> ehClient.close(), executorService).whenCompleteAsync((t, u) -> {
                    if (u != null) {
                        log.warn("closing failed with error:", u.toString());
                    }
                }, executorService).get();
            }
            executorService.shutdown();
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    private EventPosition getPosition() {
        if (AzureEventHubsInputConfiguration.ReceiverOptions.OFFSET.equals(configuration.getReceiverOptions())) {
            return EventPosition.fromOffset(configuration.getOffset(), configuration.isInclusiveFlag());
        }
        if (AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE.equals(configuration.getReceiverOptions())) {
            return EventPosition.fromSequenceNumber(configuration.getSequenceNum(), configuration.isInclusiveFlag());
        }
        if (AzureEventHubsInputConfiguration.ReceiverOptions.DATETIME.equals(configuration.getReceiverOptions())) {
            Instant enqueuedDateTime = null;
            if (configuration.getEnqueuedDateTime() == null) {
                // default query from now
                enqueuedDateTime = Instant.now();
            } else {
                enqueuedDateTime = Instant.parse(configuration.getEnqueuedDateTime());
            }
            return EventPosition.fromEnqueuedTime(enqueuedDateTime);
        }
        return EventPosition.fromStartOfStream();
    }
}