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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.eventhubs.AzureEventHubsTestBase;
import org.talend.components.azure.eventhubs.dataset.AzureEventHubsDataSet;
import org.talend.components.azure.eventhubs.source.AzureEventHubsInputConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

@Disabled("not ready")
@WithComponents("org.talend.components.azure.eventhubs")
class AzureEventHubsOutputTest extends AzureEventHubsTestBase {

    private static final String UNIQUE_ID;

    static {
        UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));
    }

    @Service
    private RecordBuilderFactory factory;

    @Test
    void testSimpleSend() {
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("0");
        dataSet.setDatastore(getDataStore());
        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.COLUMN);

        outputConfiguration.setKeyColumn("test");

        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            records.add(factory.newRecordBuilder().withString("pk", "talend_pk_1")
                    .withString("Name", "TestName_" + i + "_" + UNIQUE_ID).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();
    }

    @Test
    void testFirstColumnEmpty() {
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(EVENTHUB_NAME);
        dataSet.setPartitionId("0");
        dataSet.setDatastore(getDataStore());
        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.COLUMN);

        outputConfiguration.setKeyColumn("test");

        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>(10);
        Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(factory.newEntryBuilder().withName("Id").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("Name").withType(Schema.Type.STRING).build()).build();

        // empty value
        for (int i = 0; i < 5; i++) {
            records.add(factory.newRecordBuilder(schema).withString("Id", "")
                    .withString("Name", "TestName_" + i + "_" + UNIQUE_ID).build());
        }
        // null value
        for (int i = 5; i < 10; i++) {
            records.add(factory.newRecordBuilder(schema).withString("Name", "TestName_" + i + "_" + UNIQUE_ID).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName("consumer-group-1");
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(0L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);

        List<Record> readRecords = readByFilter(inputConfiguration, "payload", UNIQUE_ID);
        assertEquals(10, readRecords.size());
        assertEquals(0, readRecords.get(0).getString("payload").indexOf(";"));
        assertEquals(0, readRecords.get(5).getString("payload").indexOf(";"));

    }

    List<Record> readByFilter(AzureEventHubsInputConfiguration inputConfiguration, String column, String filterValue) {

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        assertNotNull(records);
        for (Record record : records) {
            System.out.println(record.getString(column));
        }
        System.out.println(filterValue);
        List<Record> filteredRecords = records.stream()
                .filter(e -> (e.getString(column) != null && e.getString(column).contains(filterValue)))
                .collect(Collectors.toList());
        getComponentsHandler().resetState();
        return filteredRecords;
    }

}