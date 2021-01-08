/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream;

import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.talend.components.common.stream.api.JsonEmitter;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.components.common.stream.input.json.JsonReaderSupplier;
import org.talend.components.common.stream.output.json.JsonWriterSupplier;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.junit.BaseComponentsHandler;
import org.talend.sdk.component.junit.environment.Environment;
import org.talend.sdk.component.junit.environment.EnvironmentConfiguration;
import org.talend.sdk.component.junit.environment.builtin.ContextualEnvironment;
import org.talend.sdk.component.junit.environment.builtin.beam.SparkRunnerEnvironment;
import org.talend.sdk.component.junit5.Injected;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.junit5.environment.EnvironmentalTest;
import org.talend.sdk.component.runtime.manager.chain.Job;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Environment(ContextualEnvironment.class)
@EnvironmentConfiguration(environment = "Contextual", systemProperties = {})

@Environment(SparkRunnerEnvironment.class)
@EnvironmentConfiguration(environment = "Spark", systemProperties = {
        @EnvironmentConfiguration.Property(key = "talend.beam.job.runner", value = "org.apache.beam.runners.spark.SparkRunner"),
        @EnvironmentConfiguration.Property(key = "talend.beam.job.filesToStage", value = ""),
        @EnvironmentConfiguration.Property(key = "spark.ui.enabled", value = "false") })

@WithComponents("org.talend.components.common.stream.api")
class JsonFormatTest {

    @Service
    private RecordIORepository repo;

    @Injected
    private BaseComponentsHandler handler;

    private JsonEmitter.Config config;

    @BeforeEach
    private void buildConfig() {
        this.config = new JsonEmitter.Config();
        this.config.setJsonPointer("/");
    }

    @EnvironmentalTest
    void format() {

        final RecordReaderSupplier reader = repo.findReader(JsonConfiguration.class);
        Assertions.assertNotNull(reader);
        Assertions.assertTrue(JsonReaderSupplier.class.isInstance(reader));

        final RecordWriterSupplier writer = repo.findWriter(JsonConfiguration.class);
        Assertions.assertNotNull(writer);
        Assertions.assertTrue(JsonWriterSupplier.class.isInstance(writer));
    }

    @EnvironmentalTest
    void testSimple() {
        config.setJsonFile("Simple.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getType());
        Assertions.assertEquals("John", record.getString("First_name"));
        Assertions.assertEquals("Doe", record.getString("Last_name"));
        Assertions.assertEquals(45, record.getInt("Age"));
    }

    @EnvironmentalTest
    void testComplex() {
        config.setJsonFile("corona-api.countries.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getType());

        // Test Array level
        Assertions.assertEquals(Schema.Type.ARRAY, record.getSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getEntries().get(0).getElementSchema().getType());

        Assertions.assertEquals(true, record.getBoolean("_cacheHit"));

        final Iterator<Record> dataIterator = record.getArray(Record.class, "data").iterator();
        final Record first = dataIterator.next();
        Assertions.assertEquals(33, first.getRecord("coordinates").getInt("latitude"));
        Assertions.assertEquals(65, first.getRecord("coordinates").getInt("longitude"));
        Assertions.assertEquals("2020-03-30T14:16:08.516Z", first.getString("updated_at"));

        // Test if missing field (null) can be retrieve by get(class, key)
        final Record second = dataIterator.next();
        Assertions.assertNull(second.getRecord("latest_data").getRecord("calculated").get(Double.class, "death_rate"));
    }

    @EnvironmentalTest
    void testArrayOfArrays() {
        config.setJsonFile("arrayOfArrays.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getType());

        final Iterator<List> data = record.getArray(List.class, "data").iterator();

        for (int i = 1; i < 4; i++) {
            final List next = data.next();
            final Iterator<String> expected = Arrays.asList("aaa" + i, "bbb" + i, "ccc" + i).iterator();
            final Iterator nested = next.iterator();
            while (nested.hasNext()) {
                Assertions.assertEquals(expected.next(), nested.next());
            }
        }
    }

    @EnvironmentalTest
    void testEmptyRecord() {
        config.setJsonFile("withEmptyRecord.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getType());

        Assertions.assertEquals("bar", record.getRecord("rec").getString("foo"));
        Assertions.assertNotNull(record.getRecord("empty_rec"));
        Assertions.assertEquals(0, record.getRecord("empty_rec").getSchema().getEntries().size());
    }

    @EnvironmentalTest
    void testLoopOnRecords() {
        config.setJsonFile("corona-api.countries.json");
        config.setJsonPointer("/data");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(2, records.size());

        final Record record1 = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record1.getSchema().getType());
        Assertions.assertEquals("Afghanistan", record1.getString("name"));

        final Record record2 = records.get(1);
        Assertions.assertEquals(Schema.Type.RECORD, record2.getSchema().getType());
        Assertions.assertEquals("Albania", record2.getString("name"));
    }

    @EnvironmentalTest
    void testLoopOnValues() {
        config.setJsonFile("ArrayOfValues.json");
        config.setJsonPointer("/data");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(4, records.size());

        final Iterator<String> expected = Arrays.asList("aaaaa", "bbbbb", "ccccc", "ddddd").iterator();
        for (Record r : records) {
            Assertions.assertEquals(expected.next(), r.getString("field"));
        }
    }

    // If an array have Heterogeneous records, it merges all schemas
    @EnvironmentalTest
    void testHeterogeneousArray() {
        config.setJsonFile("heterogeneousArray.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.ARRAY, record.getSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getEntries().get(0).getElementSchema().getType());
        Assertions.assertEquals(3, record.getSchema().getEntries().get(0).getElementSchema().getEntries().size());
    }

    @EnvironmentalTest
    void testHeterogeneousArraySameFieldWithDifferentTypes() {
        config.setJsonFile("heterogeneousArray2.json");
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.ARRAY, record.getSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getEntries().get(0).getElementSchema().getType());
        Assertions.assertEquals(4, record.getSchema().getEntries().get(0).getElementSchema().getEntries().size());
        Assertions.assertEquals(Schema.Type.STRING,
                record.getSchema().getEntries().get(0).getElementSchema().getEntries().get(2).getType());
        Assertions.assertEquals("ddd", record.getSchema().getEntries().get(0).getElementSchema().getEntries().get(2).getName());
    }

    @EnvironmentalTest
    void testNumbersInferType() {
        _testNumbers(false);
    }

    @EnvironmentalTest
    void testNumbersForceDouble() {
        _testNumbers(true);
    }

    private void _testNumbers(final boolean forceDouble) {
        config.setJsonFile("numbers.json");
        config.setForceDouble(forceDouble);
        final List<Record> records = runPipeline();
        Assertions.assertEquals(1, records.size());

        final Record record = records.get(0);
        Assertions.assertEquals(Schema.Type.RECORD, record.getSchema().getType());

        final Schema schema = record.getSchema();
        final List<Schema.Entry> entries = schema.getEntries();
        Assertions.assertEquals(7, entries.size());

        Assertions.assertEquals("is_int", entries.get(2).getName());
        Assertions.assertEquals(forceDouble ? Schema.Type.DOUBLE : Schema.Type.LONG, entries.get(2).getType());

        Assertions.assertEquals("is_a_double", entries.get(3).getName());
        Assertions.assertEquals(Schema.Type.DOUBLE, entries.get(3).getType());

        Assertions.assertEquals("is_b_double", entries.get(4).getName());
        Assertions.assertEquals(Schema.Type.DOUBLE, entries.get(4).getType());

        Assertions.assertEquals("is_array_of_int", entries.get(5).getName());
        Assertions.assertEquals(Schema.Type.ARRAY, entries.get(5).getType());
        Assertions.assertEquals(forceDouble ? Schema.Type.DOUBLE : Schema.Type.LONG, entries.get(5).getElementSchema().getType());

        Assertions.assertEquals("is_array_of_double", entries.get(6).getName());
        Assertions.assertEquals(Schema.Type.ARRAY, entries.get(6).getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, entries.get(6).getElementSchema().getType());

        final Object is_int = record.get(Object.class, "is_int");
        Assertions.assertEquals(forceDouble ? Double.class : Long.class, is_int.getClass());

        final List<Object> is_array_of_int = record.getArray(Object.class, "is_array_of_int").stream()
                .collect(Collectors.toList());
        Assertions.assertEquals(Double.class, is_array_of_int.get(2).getClass()); // must be instance of Double, not BigInteger

        final List<Object> is_array_of_double = record.getArray(Object.class, "is_array_of_double").stream()
                .collect(Collectors.toList());
        Assertions.assertEquals(Double.class, is_array_of_double.get(2).getClass()); // must be instance of Double, not BigInteger

    }

    private List<Record> runPipeline() {
        final String configStr = configurationByExample().forInstance(config).configured().toQueryString();

        Job.components() //
                .component("emitter", "jsonFamily://jsonInput?" + configStr) //
                .component("out", "test://collector") //
                .connections() //
                .from("emitter") //
                .to("out") //
                .build() //
                .run();

        return handler.getCollectedData(Record.class);
    }

}
