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
package org.talend.components.azure.eventhubs.output;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_CONSUMER_GROUP;
import static org.talend.components.azure.eventhubs.common.AzureEventHubsConstant.DEFAULT_PARTITION_ID;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.beam.sdk.Pipeline;
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

@Disabled("Run manually follow the comment")
@WithComponents("org.talend.components.azure.eventhubs")
class AzureEventHubsOutputTest extends AzureEventHubsTestBase {

    @Service
    private RecordBuilderFactory factory;

    @Test
    void testSimpleSend() {
        String partitionId = "0";
        String uniqueId = getUniqueID();
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        dataSet.setConnection(getDataStore());

        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID);
        outputConfiguration.setPartitionId(partitionId);
        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            records.add(factory.newRecordBuilder().withString("Id", String.valueOf(i))
                    .withString("Name", "TestName_" + i + "_" + uniqueId).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName(DEFAULT_CONSUMER_GROUP);
        inputConfiguration.setSpecifyPartitionId(true);
        inputConfiguration.setPartitionId(partitionId);
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(-1L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);
        List<Record> readRecords = readByFilter(inputConfiguration, "payload", uniqueId);
        assertEquals(10, readRecords.size());
        assertEquals("0;TestName_0_" + uniqueId, readRecords.get(0).getString("payload"));
        assertEquals("5;TestName_5_" + uniqueId, readRecords.get(5).getString("payload"));
    }

    @Test
    void testFirstColumnEmpty() {
        String partitionId = "1";
        String uniqueId = getUniqueID();
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        dataSet.setConnection(getDataStore());

        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID);
        outputConfiguration.setPartitionId(partitionId);
        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>(10);
        Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(factory.newEntryBuilder().withName("Id").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("Name").withType(Schema.Type.STRING).build()).build();

        // empty value
        for (int i = 0; i < 5; i++) {
            records.add(factory.newRecordBuilder(schema).withString("Id", "").withString("Name", "TestName_" + i + "_" + uniqueId)
                    .build());
        }
        // null value
        for (int i = 5; i < 10; i++) {
            records.add(factory.newRecordBuilder(schema).withString("Name", "TestName_" + i + "_" + uniqueId).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName(DEFAULT_CONSUMER_GROUP);
        inputConfiguration.setSpecifyPartitionId(true);
        inputConfiguration.setPartitionId(partitionId);
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(-1L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);

        List<Record> readRecords = readByFilter(inputConfiguration, "payload", uniqueId);
        assertEquals(10, readRecords.size());
        assertEquals(0, readRecords.get(0).getString("payload").indexOf(";"));
        assertEquals(0, readRecords.get(5).getString("payload").indexOf(";"));

    }

    @Test
    void testAllType() {
        String partitionId = "2";
        String uniqueId = getUniqueID();
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        dataSet.setConnection(getDataStore());

        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID);
        outputConfiguration.setPartitionId(partitionId);
        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>(10);
        Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(
                        factory.newEntryBuilder().withName("test_string").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("test_boolean").withType(Schema.Type.BOOLEAN).withNullable(true)
                        .build())
                .withEntry(
                        factory.newEntryBuilder().withName("test_double").withType(Schema.Type.DOUBLE).withNullable(true).build())
                .withEntry(
                        factory.newEntryBuilder().withName("test_float").withType(Schema.Type.FLOAT).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("test_long").withType(Schema.Type.LONG).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("test_int").withType(Schema.Type.INT).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("test_datetime").withType(Schema.Type.DATETIME).withNullable(true)
                        .build())
                .withEntry(
                        factory.newEntryBuilder().withName("test_bytes").withType(Schema.Type.BYTES).withNullable(true).build())
                .build();

        // empty value
        records.add(factory.newRecordBuilder(schema).withString("test_string", "test_string_" + uniqueId)
                .withBoolean("test_boolean", false).withDouble("test_double", 0.25).withFloat("test_float", 0.25f)
                .withLong("test_long", 1000L).withInt("test_int", 100)
                .withDateTime("test_datetime", Calendar.getInstance().getTime())
                .withBytes("test_bytes", "test_bytes".getBytes(Charset.forName("UTF-8"))).build());
        // null value
        records.add(factory.newRecordBuilder(schema).withString("test_string", "test_string_" + uniqueId).build());

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName(DEFAULT_CONSUMER_GROUP);
        inputConfiguration.setSpecifyPartitionId(true);
        inputConfiguration.setPartitionId(partitionId);
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(-1L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);

        List<Record> readRecords = readByFilter(inputConfiguration, "payload", uniqueId);
        assertEquals(2, readRecords.size());

        assertEquals(0, readRecords.get(0).getString("payload").indexOf("test_string_" + uniqueId + ";false"));
        assertEquals(0, readRecords.get(1).getString("payload").indexOf("test_string_" + uniqueId + ";;;;;;;"));

    }

    @Test
    void testUnSupportedType() {
        assertThrows(IllegalStateException.class, () -> {
            try {
                String uniqueId = getUniqueID();
                AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
                final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
                dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
                dataSet.setConnection(getDataStore());

                outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.SPECIFY_PARTITION_ID);
                outputConfiguration.setPartitionId(DEFAULT_PARTITION_ID);
                outputConfiguration.setDataset(dataSet);

                List<Record> records = Collections
                        .singletonList(factory.newRecordBuilder().withString("test_string", "test_string_" + uniqueId)
                                .withArray(
                                        factory.newEntryBuilder().withName("test_array")
                                                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build())
                                                .withType(Schema.Type.ARRAY).withNullable(true).build(),
                                        Collections.singleton("test_array"))
                                .build());

                final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
                getComponentsHandler().setInputData(records);
                Job.components().component("emitter", "test://emitter")
                        .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                        .from("emitter").to("azureeventhubs-output").build().run();
                getComponentsHandler().resetState();
            } catch (Pipeline.PipelineExecutionException e) {
                throw e.getCause();
            }
        });
    }

    @Test
    void testPartitionKeyROUND_ROBIN() {
        String uniqueId = getUniqueID();
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        dataSet.setConnection(getDataStore());

        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.ROUND_ROBIN);
        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            records.add(factory.newRecordBuilder().withString("Id", String.valueOf(i))
                    .withString("Name", "TestName_" + i + "_" + uniqueId).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?" + config).connections()
                .from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName(DEFAULT_CONSUMER_GROUP);
        // consume messages from all partitions
        inputConfiguration.setSpecifyPartitionId(false);
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(-1L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);
        List<Record> readRecords = readByFilter(inputConfiguration, "payload", uniqueId);
        assertEquals(100, readRecords.size());
    }

    @Test
    void testPartitionKeyColumn() {
        String uniqueId = getUniqueID();
        AzureEventHubsOutputConfiguration outputConfiguration = new AzureEventHubsOutputConfiguration();
        final AzureEventHubsDataSet dataSet = new AzureEventHubsDataSet();
        dataSet.setEventHubName(SHARED_EVENTHUB_NAME);
        dataSet.setConnection(getDataStore());

        outputConfiguration.setPartitionType(AzureEventHubsOutputConfiguration.PartitionType.COLUMN);
        outputConfiguration.setKeyColumn("Id");
        outputConfiguration.setDataset(dataSet);

        List<Record> records = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            // every 20 records have same Id value
            records.add(factory.newRecordBuilder().withString("Id", String.valueOf((i / 20) + 1))
                    .withString("Name", "TestName_" + i + "_" + uniqueId).build());
        }

        final String config = configurationByExample().forInstance(outputConfiguration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("azureeventhubs-output", "AzureEventHubs://AzureEventHubsOutput?$maxBatchSize=20&" + config)
                .connections().from("emitter").to("azureeventhubs-output").build().run();
        getComponentsHandler().resetState();

        // read record
        AzureEventHubsInputConfiguration inputConfiguration = new AzureEventHubsInputConfiguration();
        inputConfiguration.setConsumerGroupName(DEFAULT_CONSUMER_GROUP);
        // consume messages from all partitions
        inputConfiguration.setSpecifyPartitionId(false);
        inputConfiguration.setReceiverOptions(AzureEventHubsInputConfiguration.ReceiverOptions.SEQUENCE);
        inputConfiguration.setSequenceNum(-1L);
        inputConfiguration.setDataset(dataSet);
        inputConfiguration.setReceiveTimeout(10L);
        List<Record> readRecords = readByFilter(inputConfiguration, "payload", uniqueId);
        assertEquals(100, readRecords.size());
    }

    List<Record> readByFilter(AzureEventHubsInputConfiguration inputConfiguration, String column, String filterValue) {

        final String config = configurationByExample().forInstance(inputConfiguration).configured().toQueryString();
        Job.components().component("azureeventhubs-input", "AzureEventHubs://AzureEventHubsInputMapper?" + config)
                .component("collector", "test://collector").connections().from("azureeventhubs-input").to("collector").build()
                .run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);
        assertNotNull(records);
        List<Record> filteredRecords = records.stream()
                .filter(e -> (e.getString(column) != null && e.getString(column).contains(filterValue)))
                .collect(Collectors.toList());
        getComponentsHandler().resetState();
        return filteredRecords;
    }

}