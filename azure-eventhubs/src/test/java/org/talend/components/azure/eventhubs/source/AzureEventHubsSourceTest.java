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

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.eventhubs.AzureEventHubsTestBase;
import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@Disabled("not ready")
@WithComponents("org.talend.components.azure.eventhubs")
class AzureEventHubsSourceTest extends AzureEventHubsTestBase {

    @Test
    void testReadByOffset() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("1");
        inputConfiguration.setConsumerGroupName("consumer-group-1");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.OFFSET);
        inputConfiguration.setOffset("-1");
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
    }

    @Test
    void testReadBySeq() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("1");

        inputConfiguration.setConsumerGroupName("consumer-group-1");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(0L);
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
    }

    @Test
    void testReadByDateTime() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("2");

        inputConfiguration.setConsumerGroupName("consumer-group-1");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.DATETIME);
        inputConfiguration.setEnqueuedDateTime("2019-04-04T00:00:00.000Z");
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
    }

    @Test
    void testReadByDateTimeNotSet() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("2");

        inputConfiguration.setConsumerGroupName("consumer-group-1");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.DATETIME);
        inputConfiguration.setUseMaxNum(true);
        inputConfiguration.setMaxNumReceived(5L);
        inputConfiguration.setReceiveTimeout(10L);
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertNotNull(records);
        Assert.assertEquals(5, records.size());
    }

    @Test
    void testReadTimeOut() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("1");
        inputConfiguration.setConsumerGroupName("$Default");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.OFFSET);
        inputConfiguration.setOffset("-1");
        inputConfiguration.setReceiveTimeout(60L);
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
    }

    @Test
    void testReadMaxNumReceived() {
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setDatastore(getDataStore());
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("1");
        inputConfiguration.setConsumerGroupName("$Default");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.OFFSET);
        inputConfiguration.setOffset("-1");
        inputConfiguration.setUseMaxNum(true);
        inputConfiguration.setMaxNumReceived(10L);
        inputConfiguration.setDataset(dataSet);

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        Assert.assertNotNull(records);
        Assert.assertEquals(10, records.size());
    }

}