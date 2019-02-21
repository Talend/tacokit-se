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

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.commons.lang3.RandomStringUtils;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.azure.common.AzureConnection;
import org.talend.components.azure.common.AzureTableConnection;
import org.talend.components.azure.common.Comparison;
import org.talend.components.azure.service.AzureComponentServices;
import org.talend.components.azure.service.AzureConnectionService;
import org.talend.components.azure.table.output.OutputProperties;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.maven.MavenDecrypter;
import org.talend.sdk.component.maven.Server;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.table.CloudTable;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.azure")
public class AzureInputITTest {

    private static final int DEFAULT_TABLE_SIZE = 15;

    @Service
    private AzureComponentServices componentService;

    @Service
    private AzureConnectionService connectionService;

    @ClassRule
    public static final SimpleComponentRule COMPONENT = new SimpleComponentRule("org.talend.components.azure");

    private static InputProperties inputProperties;

    private String tableName;

    private CloudStorageAccount storageAccount;

    @BeforeEach
    public void init() throws Exception {
        tableName = "TestIT" + RandomStringUtils.randomAlphanumeric(10);

        Server account;
        AzureTableConnection dataSet = new AzureTableConnection();
        AzureConnection dataStore = new AzureConnection();
        final MavenDecrypter decrypter = new MavenDecrypter();
        account = decrypter.find("azure.account");
        dataStore.setAccountName(account.getUsername());
        dataStore.setAccountKey(account.getPassword());

        dataSet.setConnection(dataStore);
        dataSet.setTableName(tableName);
        inputProperties = new InputProperties();
        inputProperties.setAzureConnection(dataSet);

        storageAccount = connectionService.createStorageAccount(inputProperties.getAzureConnection().getConnection());

        connectionService.createTable(storageAccount, tableName);
        populateTable();
    }

    @Test
    public void selectAllInputPipelineTest() {
        Schema tableSchema = componentService.guessSchema(inputProperties.getAzureConnection());
        inputProperties.setSchema(tableSchema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("azureInput", "AzureStorage://InputTable?" + inputConfig)
                .component("collector", "test://collector").connections().from("azureInput").to("collector").build().run();

        List<Record> records = COMPONENT.getCollectedData(Record.class);

        Assertions.assertEquals(DEFAULT_TABLE_SIZE, records.size());
    }

    @Test
    public void useFilterExpressionWhileSelect() {
        InputProperties.FilterExpression testFilterExpression = new InputProperties.FilterExpression();
        testFilterExpression.setColumn("intValue");
        testFilterExpression.setFieldType(InputProperties.FieldType.NUMERIC);
        testFilterExpression.setFunction(Comparison.EQUAL);
        testFilterExpression.setValue("1");

        Schema tableSchema = componentService.guessSchema(inputProperties.getAzureConnection());
        inputProperties.setSchema(tableSchema.getEntries().stream().map(Schema.Entry::getName).collect(Collectors.toList()));
        inputProperties.setUseFilterExpression(true);
        inputProperties.setFilterExpressions(Collections.singletonList(testFilterExpression));

        String inputConfig = configurationByExample().forInstance(inputProperties).configured().toQueryString();
        Job.components().component("azureInput", "AzureStorage://InputTable?" + inputConfig)
                .component("collector", "test://collector").connections().from("azureInput").to("collector").build().run();

        List<Record> records = COMPONENT.getCollectedData(Record.class);

        Assertions.assertEquals(1, records.size());
    }

    private void populateTable() {
        OutputProperties outputProperties = new OutputProperties();
        List<String> schema = new ArrayList<>();
        schema.add("PartitionKey");
        schema.add("RowKey");
        schema.add("booleanValue");
        schema.add("longValue");
        schema.add("intValue");
        schema.add("doubleValue");
        schema.add("dateValue");

        outputProperties.setAzureConnection(inputProperties.getAzureConnection());
        outputProperties.setPartitionName("PartitionKey");
        outputProperties.setRowKey("RowKey");

        outputProperties.setSchema(schema);
        List<Record> defaultRecordsList = IntStream.range(0, DEFAULT_TABLE_SIZE)
                .mapToObj(i -> COMPONENT.findService(RecordBuilderFactory.class).newRecordBuilder()
                        .withString("PartitionKey", "testKey" + i).withString("RowKey", "SomeKey" + i)
                        .withBoolean("booleanValue", true).withLong("longValue", 1L).withInt("intValue", i)
                        .withDouble("doubleValue", 0.0).withDateTime("dateValue", Date.from(Instant.now())).build())
                .collect(Collectors.toList());

        COMPONENT.setInputData(defaultRecordsList);

        String outputConfig = configurationByExample().forInstance(outputProperties).configured().toQueryString();
        Job.components().component("inputFlow", "test://emitter")
                .component("outputComponent", "AzureStorage://OutputTable?" + outputConfig).connections().from("inputFlow")
                .to("outputComponent").build().run();
    }

    @AfterEach
    public void releaseTable() throws Exception {
        CloudTable cloudTable = storageAccount.createCloudTableClient().getTableReference(tableName);
        cloudTable.delete(null, AzureConnectionService.getTalendOperationContext());
    }

}
