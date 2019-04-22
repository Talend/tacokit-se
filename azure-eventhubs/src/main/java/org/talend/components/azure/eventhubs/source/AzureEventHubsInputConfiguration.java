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

import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.condition.ActiveIfs;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "consumerGroupName" }), @GridLayout.Row({ "receiverOptions" }),
        @GridLayout.Row({ "offset" }), @GridLayout.Row({ "sequenceNum", "inclusiveFlag" }),
        @GridLayout.Row({ "enqueuedDateTime" }), @GridLayout.Row({ "useMaxNum" }), @GridLayout.Row({ "maxNumReceived" }),
        @GridLayout.Row({ "receiveTimeout" }) })
@Documentation("Consume message from eventhubs configuration")
public class AzureEventHubsInputConfiguration implements Serializable {

    private static final String DEFAULT_CONSUMER_GROUP = "$Default";

    @Option
    @Documentation("The dataset to consume")
    private AzureEventHubsDataSet dataset;

    @Option
    @Documentation("The consumer group name that this receiver should be grouped under")
    private String consumerGroupName = DEFAULT_CONSUMER_GROUP;

    @Option
    @Documentation("If offsets don't already exist, where to start reading in the topic.")
    private ReceiverOptions receiverOptions = ReceiverOptions.OFFSET;

    @Option
    @ActiveIf(target = "receiverOptions", value = "OFFSET")
    @Proposable("PROPOSABLE_OFFSET")
    @Documentation("The byte offset of the event.\n" + " \"-1\" is the start of a partition stream in EventHub.\n"
            + "\"@latest\" current end of a partition stream in EventHub")
    private String offset = "-1";

    @Option
    @ActiveIf(target = "receiverOptions", value = "SEQUENCE")
    @Documentation("The sequence number of the event")
    private Long sequenceNum = 0L;

    @Option
    @ActiveIf(target = "receiverOptions", value = { "OFFSET", "SEQUENCE" })
    @Documentation("Will include the specified event when set to true; otherwise, the next event is returned")
    private boolean inclusiveFlag;

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
    private Long receiveTimeout = 60L;

    public enum ReceiverOptions {
        OFFSET,
        SEQUENCE,
        DATETIME
    }

}