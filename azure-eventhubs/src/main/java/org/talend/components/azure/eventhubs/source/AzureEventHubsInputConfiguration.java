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
package org.talend.components.azure.eventhubs.source;

import java.io.Serializable;

import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_CONSUMER_GROUP;
import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_PARTITION_ID;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "specifyPartitionId" }), @GridLayout.Row({ "partitionId" }),
        @GridLayout.Row({ "consumerGroupName" }), @GridLayout.Row({ "receiverOptions" }), @GridLayout.Row({ "offset" }),
        @GridLayout.Row({ "sequenceNum", "inclusiveFlag" }), @GridLayout.Row({ "enqueuedDateTime" }),
        @GridLayout.Row({ "useMaxNum" }), @GridLayout.Row({ "maxNumReceived" }), @GridLayout.Row({ "receiveTimeout" }) })
@Documentation("Consume message from eventhubs configuration")
public class AzureEventHubsInputConfiguration implements Serializable {

    @Option
    @Documentation("The dataset to consume")
    private AzureEventHubsDataSet dataset;

    @Option
    @Documentation("Whether use specified partition id, else scan all partitions")
    private boolean specifyPartitionId;

    @Option
    @ActiveIf(target = "specifyPartitionId", value = "true")
    @DefaultValue(DEFAULT_PARTITION_ID)
    @Suggestable(value = "listPartitionIds", parameters = { "../dataset" })
    @Documentation("The partition Id that the receiver belongs to. All data received will be from this partition only")
    private String partitionId;

    @Option
    @Documentation("The consumer group name that this receiver should be grouped under")
    private String consumerGroupName = DEFAULT_CONSUMER_GROUP;

    @Option
    @Documentation("If offsets don't already exist, where to start reading in the topic.")
    private ReceiverOptions receiverOptions = ReceiverOptions.OFFSET;

    @Option
    @ActiveIf(target = "receiverOptions", value = "OFFSET")
    @Documentation("START_OF_STREAM means from the beginning of offset, END_OF_STREAM means from the latest offset")
    private EventOffsetPosition offset = EventOffsetPosition.START_OF_STREAM;

    @Option
    @Min(-1)
    @ActiveIf(target = "receiverOptions", value = "SEQUENCE")
    @Documentation("The sequence number of the event")
    private Long sequenceNum = -1L;

    @Option
    @ActiveIf(target = "receiverOptions", value = "SEQUENCE")
    @Documentation("Will include the specified event when set to true; otherwise, the next event is returned")
    private boolean inclusiveFlag = true;

    @Option
    @ActiveIf(target = "receiverOptions", value = "DATETIME")
    @Documentation("DateTime is the enqueued time of the event")
    private String enqueuedDateTime;

    @Option
    @Documentation("Will include the specified event when set to true; otherwise, the next event is returned")
    private boolean useMaxNum;

    @Option
    @Min(1)
    @ActiveIf(target = "useMaxNum", value = "true")
    @Documentation("maximum number of messages")
    private Long maxNumReceived = 5000L;

    @Option
    @Min(0)
    @Documentation("Receive timeout seconds")
    private Long receiveTimeout = 20L;

    public enum ReceiverOptions {
        OFFSET,
        SEQUENCE,
        DATETIME
    }

    public enum EventOffsetPosition {
        START_OF_STREAM,
        End_OF_STREAM,
    }

}