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

import java.io.Serializable;

import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Proposable;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "partitionType" }), @GridLayout.Row({ "keyColumn" }),
        @GridLayout.Row({ "partitionId" }) })
@Documentation("Consume message from eventhubs configuration")
public class AzureEventHubsOutputConfiguration implements Serializable {

    @Option
    @Documentation("The dataset to consume")
    private AzureEventHubsDataSet dataset;

    @Option
    @Required
    @Documentation("Strategy for assigning partitions")
    private PartitionType partitionType = PartitionType.ROUND_ROBIN;

    @Option
    @ActiveIf(target = "partitionType", value = "COLUMN")
    @Proposable("INCOMING_PATHS_DYNAMIC")
    @Documentation("Partition by hash value of this key column")
    private String keyColumn;

    @Option
    @ActiveIf(target = "partitionType", value = "SPECIFY_PARTITION_ID")
    @DefaultValue("0")
    @Suggestable(value = "listPartitionIds", parameters = { "../dataset" })
    @Documentation("The partition Id that the receiver belongs to. All data received will be from this partition only")
    private String partitionId;

    public enum PartitionType {
        // no key provided, use default partition strategy
        ROUND_ROBIN,
        // use the value of one column in the record as the key, would use this value to calculate partition
        COLUMN,
        // send message to specified partition
        SPECIFY_PARTITION_ID,
    }

}