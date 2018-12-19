/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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

package org.talend.components.salesforce.output;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.talend.components.salesforce.service.SalesforceService.URL;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.beam.sdk.Pipeline;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.salesforce.SalesforceTestBase;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.datastore.BasicDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;
import org.talend.sdk.component.runtime.output.Processor;

@Disabled("Salesforce credentials is not ready on ci")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@WithComponents("org.talend.components.salesforce")
public class SalesforceOutputProcessorTest extends SalesforceTestBase {

    private static final String UNIQUE_ID;

    static {
        UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));
    }

    @Service
    private RecordBuilderFactory factory;

    @BeforeAll
    public void prepareData() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = getComponentsHandler().createProcessor(SalesforceOutput.class, configuration);

        RecordBuilderFactory factory = getComponentsHandler().findService(RecordBuilderFactory.class);
        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            records.add(factory.newRecordBuilder().withString("Name", "TestName_" + i + "_" + UNIQUE_ID).build());
        }

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(records);
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + config).connections().from("emitter")
                .to("salesforce-output").build().run();
        getComponentsHandler().resetState();
        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 10);
    }

    @Test
    @DisplayName("Test insert supported types")
    public void testInsertTypes() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Contact");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setBatchMode(false);
        configuration.setExceptionForErrors(true);

        // We create the component processor instance using the configuration filled above
        final Processor processor = getComponentsHandler().createProcessor(SalesforceOutput.class, configuration);

        Record record = factory.newRecordBuilder().withString("FirstName", "F_test_types_" + UNIQUE_ID)
                .withString("LastName", "F_test_types_" + UNIQUE_ID).withString("Email", "testalltype_" + UNIQUE_ID + "@test.com")
                .withDouble("MailingLongitude", 115.7).withDouble("MailingLatitude", 39.4).withDateTime("Birthdate", new Date())
                .build();
        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(asList(record));
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + config).connections().from("emitter")
                .to("salesforce-output").build().run();
        getComponentsHandler().resetState();
        checkModuleData("Contact",
                "MailingLongitude != null and MailingLongitude !=null and Birthdate !=null and Name Like 'F_test_types_%"
                        + UNIQUE_ID + "%'",
                1);
    }

    @Test
    @DisplayName("Test insert object without field value set")
    public void testInsertNoFieldSet() {
        final BasicDataStore datasore = new BasicDataStore();
        datasore.setEndpoint(URL);
        datasore.setUserId(USER_ID);
        datasore.setPassword(PASSWORD);
        datasore.setSecurityKey(SECURITY_KEY);

        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(datasore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(false);

        // create a record with no field match module fields
        Record record = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_Wrong_Field_Name_" + UNIQUE_ID).build();

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();

        getComponentsHandler().setInputData(asList(record));
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + config).connections().from("emitter")
                .to("salesforce-output").build().run();
        getComponentsHandler().resetState();
        checkModuleData("Account", "Name Like 'test_Wrong_Field_Name_%" + UNIQUE_ID + "%'", 0);
    }

    @Test
    @DisplayName("Test exception on error")
    public void testExceptionOnError() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(true);

        Record record = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_" + UNIQUE_ID).build();

        final String config = configurationByExample().forInstance(configuration).configured().toQueryString();

        getComponentsHandler().setInputData(asList(record));
        // run the pipeline and ensure the execution was successful
        assertThrows(IllegalStateException.class,
                () -> Job.components().component("emitter", "test://emitter")
                        .component("salesforce-output", "Salesforce://SalesforceOutput?" + config).connections().from("emitter")
                        .to("salesforce-output").build().run());
        getComponentsHandler().resetState();
    }

    @Test
    @DisplayName("Test update object with `Id`")
    public void testUpdate() {
        // 1. read data from salesforce
        final ModuleDataSet inputDataSet = new ModuleDataSet();
        inputDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "Name"));
        inputDataSet.setColumnSelectionConfig(selectionConfig);
        inputDataSet.setDataStore(getDataStore());
        inputDataSet.setCondition("Name Like '%" + UNIQUE_ID + "%'");

        final String inputConfig = configurationByExample().forInstance(inputDataSet).configured().toQueryString();
        Job.components().component("salesforce-input", "Salesforce://ModuleQueryInput?" + inputConfig)
                .component("collector", "test://collector").connections().from("salesforce-input").to("collector").build().run();
        final List<Record> records = getComponentsHandler().getCollectedData(Record.class);

        // 2. update record
        final List<Record> updateRecord = new ArrayList<>();
        records.stream().forEach(record -> updateRecord.add(factory.newRecordBuilder().withString("Id", record.getString("Id"))
                .withString("Name", record.getString("Name").replace("TestName", "TestName_update")).build()));

        getComponentsHandler().resetState();
        // 3.write updated records to salesforce
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet outDataSet = new ModuleDataSet();
        outDataSet.setModuleName("Account");
        outDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPDATE);
        configuration.setModuleDataSet(outDataSet);

        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(updateRecord);
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + outputConfig).connections().from("emitter")
                .to("salesforce-output").build().run();
        getComponentsHandler().resetState();

        // 4. check the update
        checkModuleData("Account", "Name Like 'TestName_update%" + UNIQUE_ID + "%'", 10);
    }

    @Test
    @DisplayName("Test upsert with upsert key")
    public void testUpsert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Contact");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPSERT);
        configuration.setUpsertKeyColumn("Email");
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setBatchMode(false);

        Record record = factory.newRecordBuilder().withString("Email", "aaa_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F1_UPDATE_" + UNIQUE_ID).withString("LastName", "L1_UPDATE_" + UNIQUE_ID).build();

        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(asList(record));
        Job.components().component("emitter", "test://emitter")
                .component("salesforce-output", "Salesforce://SalesforceOutput?" + outputConfig).connections().from("emitter")
                .to("salesforce-output").build().run();

        getComponentsHandler().resetState();
        checkModuleData("Contact", "FirstName = 'F1_UPDATE_" + UNIQUE_ID + "'", 1);
    }

    @Test
    @DisplayName("Test batch mode with exception on error checked")
    public void testBatchModeExceptionOnError() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(getDataStore());
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(true);
        configuration.setBatchMode(true);
        configuration.setCommitLevel(2);

        Record record_1 = factory.newRecordBuilder().withString("Name", "test_batch_1_" + UNIQUE_ID).build();
        Record record_2 = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_batch_2_" + UNIQUE_ID).build();
        Record record_3 = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_batch_3_" + UNIQUE_ID).build();

        final String outputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        getComponentsHandler().setInputData(asList(record_1, record_2, record_3));
        assertThrows(Pipeline.PipelineExecutionException.class,
                () -> Job.components().component("emitter", "test://emitter")
                        .component("salesforce-output", "Salesforce://SalesforceOutput?" + outputConfig).connections()
                        .from("emitter").to("salesforce-output").build().run());

        checkModuleData("Account", "Name Like 'test_batch_%" + UNIQUE_ID + "%'", 1);
        getComponentsHandler().resetState();
    }

    @AfterAll
    public void cleanupTestRecords() {
        cleanTestRecords("Account", "Name Like '%" + UNIQUE_ID + "%'");
        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 0);

        cleanTestRecords("Contact", "Email Like '%" + UNIQUE_ID + "%'");
        checkModuleData("Contact", "Email Like '%" + UNIQUE_ID + "%'", 0);
    }

}