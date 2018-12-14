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
import static java.util.Collections.emptyMap;
import static org.apache.ziplock.JarLocation.jarLocation;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.runners.MethodSorters.NAME_ASCENDING;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.util.Utf8;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.values.PCollection;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.talend.components.salesforce.SalesforceTestBase;
import org.talend.components.salesforce.dataset.ModuleDataSet;
import org.talend.components.salesforce.input.ModuleQueryEmitter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.beam.Data;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.TalendFn;
import org.talend.sdk.component.runtime.beam.TalendIO;
import org.talend.sdk.component.runtime.beam.spi.record.AvroRecord;
import org.talend.sdk.component.runtime.beam.transform.ViewsMappingTransform;
import org.talend.sdk.component.runtime.input.Mapper;
import org.talend.sdk.component.runtime.output.Branches;
import org.talend.sdk.component.runtime.output.InputFactory;
import org.talend.sdk.component.runtime.output.OutputFactory;
import org.talend.sdk.component.runtime.output.Processor;

@FixMethodOrder(NAME_ASCENDING)
@WithComponents("org.talend.components.salesforce")
public class SalesforceOutputBeamTest extends SalesforceTestBase {

    private static final String PLUGIN = jarLocation(SalesforceOutputBeamTest.class).getAbsolutePath();

    private static final String UNIQUE_ID = Integer.toString(ThreadLocalRandom.current().nextInt(1, 100000));

    private RecordBuilderFactory factory = COMPONENT_FACTORY.findService(RecordBuilderFactory.class);

    @Test
    public void test001_Insert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        List<Record> records = new ArrayList<>(10);
        for (int i = 0; i < 10; i++) {
            records.add(factory.newRecordBuilder().withString("Name", "TestName_" + i + "_" + UNIQUE_ID).build());
        }

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", records);

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 10);
    }

    @Test
    public void test002_InsertNoFieldValueSet() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(false);

        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        // create a record with no field match module fields
        Record record = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_" + UNIQUE_ID).build();

        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }

    @Test
    public void test003_ExceptionOnError() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(true);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_" + UNIQUE_ID).build();

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertThrows(IllegalStateException.class, () -> pipeline.run().waitUntilFinish());
    }

    @Test
    public void test004_Update() {
        final ModuleDataSet inputDataSet = new ModuleDataSet();
        inputDataSet.setModuleName("Account");
        ModuleDataSet.ColumnSelectionConfig selectionConfig = new ModuleDataSet.ColumnSelectionConfig();
        selectionConfig.setSelectColumnNames(Arrays.asList("Id", "Name"));
        inputDataSet.setColumnSelectionConfig(selectionConfig);
        inputDataSet.setDataStore(dataStore);
        inputDataSet.setCondition("Name Like '%" + UNIQUE_ID + "%'");

        final Mapper mapper = COMPONENT_FACTORY.createMapper(ModuleQueryEmitter.class, inputDataSet);

        // output
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet outDataSet = new ModuleDataSet();
        outDataSet.setModuleName("Account");
        outDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPDATE);
        configuration.setModuleDataSet(outDataSet);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        ////////////////////////////////////////////////////////////////////////////////////////////////////////////

        // Create beam input from mapper and apply input to pipeline
        pipeline.apply(TalendIO.read(mapper)).apply(new ViewsMappingTransform(emptyMap(), PLUGIN))
                .apply(TalendFn.asFn(new BaseTestProcessor() {

                    @Override
                    public void onNext(final InputFactory input, final OutputFactory outputFactory) {
                        final Record record = (Record) input.read(Branches.DEFAULT_BRANCH);
                        outputFactory.create(Branches.DEFAULT_BRANCH)
                                .emit(factory.newRecordBuilder().withString("Id", record.getString("Id"))
                                        .withString("Name", record.getString("Name").replace("TestName", "TestName_update"))
                                        .build());
                    }
                }))
                // .apply(new ViewsMappingTransform(emptyMap(), PLUGIN))
                .apply(TalendIO.write(processor));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

        checkModuleData("Account", "Name Like 'TestName_update%" + UNIQUE_ID + "%'", 10);
    }

    @Test
    public void test005_Upsert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Contact");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPSERT);
        configuration.setUpsertKeyColumn("Email");
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setBatchMode(false);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record_1 = factory.newRecordBuilder().withString("Email", "aaa_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F1_" + UNIQUE_ID).withString("LastName", "L1_" + UNIQUE_ID).build();
        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record_1));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

        checkModuleData("Contact", "FirstName = 'F1_" + UNIQUE_ID + "'", 1);
    }

    @Test
    public void test006_Upsert() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Contact");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.UPSERT);
        configuration.setUpsertKeyColumn("Email");
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setBatchMode(false);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record = factory.newRecordBuilder().withString("Email", "aaa_" + UNIQUE_ID + "@test.com")
                .withString("FirstName", "F1_UPDATE_" + UNIQUE_ID).withString("LastName", "L1_UPDATE_" + UNIQUE_ID).build();

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(record));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

        checkModuleData("Contact", "FirstName = 'F1_UPDATE_" + UNIQUE_ID + "'", 1);
    }

    @Test
    public void test007_BatchModeExceptionOnError() {
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(true);
        configuration.setBatchMode(true);
        configuration.setCommitLevel(2);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        Record record_1 = factory.newRecordBuilder().withString("Name", "test_batch_1_" + UNIQUE_ID).build();
        Record record_2 = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_batch_2_" + UNIQUE_ID).build();
        Record record_3 = factory.newRecordBuilder().withString("Wrong_Field_Name", "test_batch_3_" + UNIQUE_ID).build();
        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(record_1, record_2, record_3));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));

        // run the pipeline and ensure the execution was successful
        assertThrows(IllegalStateException.class, () -> pipeline.run().waitUntilFinish());

        checkModuleData("Account", "Name Like 'test_batch_%" + UNIQUE_ID + "%'", 1);
    }

    @Test
    public void test008_RecordIncludeAvroUtf8() {
        Schema recordSchema = SchemaBuilder.builder().record("Schema").fields() //
                .name("Name").type().nullable().stringType().noDefault() //
                .name("Phone").type().nullable().stringType().noDefault() //
                .endRecord();
        final OutputConfiguration configuration = new OutputConfiguration();
        final ModuleDataSet moduleDataSet = new ModuleDataSet();
        moduleDataSet.setModuleName("Account");
        moduleDataSet.setDataStore(dataStore);
        configuration.setOutputAction(OutputConfiguration.OutputAction.INSERT);
        configuration.setModuleDataSet(moduleDataSet);
        configuration.setExceptionForErrors(true);
        configuration.setBatchMode(true);
        configuration.setCommitLevel(2);

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(SalesforceOutput.class, configuration);

        IndexedRecord r1 = new GenericData.Record(recordSchema);
        r1.put(0, new Utf8("test_batch_1_" + "_" + UNIQUE_ID));
        r1.put(1, UNIQUE_ID);
        IndexedRecord r2 = new GenericData.Record(recordSchema);
        r2.put(0, new Utf8("test_batch_2_" + "_" + UNIQUE_ID));
        r2.put(1, UNIQUE_ID);
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
                asList(new AvroRecord(r1), new AvroRecord(r2)));

        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));

        // add our processor right after to see each data as configured previously
        inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), AvroRecord.class));

        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());

        checkModuleData("Account", "Name Like 'test_batch_%" + UNIQUE_ID + "%' and Phone = '" + UNIQUE_ID + "'", 2);

    }

    @Test
    public void test998_cleanupModuleAccount() {
        cleanTestRecords("Account", "Name Like '%" + UNIQUE_ID + "%'", PLUGIN);
        checkModuleData("Account", "Name Like '%" + UNIQUE_ID + "%'", 0);
    }

    @Test
    public void test999_cleanupModuleContact() {
        cleanTestRecords("Contact", "Email Like '%" + UNIQUE_ID + "%'", PLUGIN);
        checkModuleData("Contact", "Email Like '%" + UNIQUE_ID + "%'", 0);
    }

    private static abstract class BaseTestProcessor implements Serializable, Processor {

        @Override
        public void beforeGroup() {
            // no-op
        }

        @Override
        public void afterGroup(final OutputFactory output) {
            // no-op
        }

        @Override
        public String plugin() {
            return PLUGIN;
        }

        @Override
        public String rootName() {
            return "test-classes";
        }

        @Override
        public String name() {
            return "test-classes";
        }

        @Override
        public void start() {
            // no-op
        }

        @Override
        public void stop() {
            // no-op
        }
    }

}