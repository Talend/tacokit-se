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
package org.talend.components.couchbase.source;

import com.couchbase.client.deps.io.netty.buffer.ByteBuf;
import com.couchbase.client.deps.io.netty.buffer.Unpooled;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.BinaryDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.TestData;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.dataset.DocumentType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@Slf4j
@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseInput component")
public class CouchbaseInputTest extends CouchbaseUtilTest {

    private void executeJob(CouchbaseInputConfiguration configuration) {
        final String inputConfig = configurationByExample().forInstance(configuration).configured().toQueryString();
        Job.components().component("Couchbase_Input", "Couchbase://Input?" + inputConfig)
                .component("collector", "test://collector").connections().from("Couchbase_Input").to("collector").build().run();
    }

    @Test
    @DisplayName("Check input data")
    void couchbaseInputDataTest() {
        log.info("Test start: couchbaseInputDataTest");
        String idPrefix = "couchbaseInputDataTest";
        insertTestDataToDB(idPrefix);
        executeJob(getInputConfiguration());

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("_meta_id_").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("_meta_id_"))).collect(Collectors.toList());
        assertEquals(2, data.size());

        assertOneRecord("1", data.get(0));
        assertOneRecord("2", data.get(1));
    }

    private void insertTestDataToDB(String idPrefix) {
        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);

        List<JsonObject> jsonObjects = createJsonObjects();
        for (int i = 0; i < 2; i++) {
            bucket.insert(JsonDocument.create(generateDocId(idPrefix, i), jsonObjects.get(i)));
        }
        bucket.close();
    }

    private List<JsonObject> createJsonObjects() {
        TestData testData = new TestData();
        List<JsonObject> jsonObjects = new ArrayList<>();
        for (int i = 1; i <= 2; i++) {
            jsonObjects.add(createJsonObject(testData.getColId() + i));
        }
        return jsonObjects;
    }

    private JsonObject createJsonObject(String id) {
        TestData testData = new TestData();
        JsonObject json = JsonObject.create().put("t_string", id).put("t_int_min", testData.getColIntMin())
                .put("t_int_max", testData.getColIntMax()).put("t_long_min", testData.getColLongMin())
                .put("t_long_max", testData.getColLongMax()).put("t_float_min", testData.getColFloatMin())
                .put("t_float_max", testData.getColFloatMax()).put("t_double_min", testData.getColDoubleMin())
                .put("t_double_max", testData.getColDoubleMax()).put("t_boolean", testData.isColBoolean())
                .put("t_datetime", testData.getColDateTime().toString()).put("t_array", testData.getColList());
        return json;
    }

    @Test
    @DisplayName("When input data is null, record will be skipped")
    void firstValueIsNullInInputDBTest() {
        log.info("Test start: firstValueIsNullInInputDBTest");
        String idPrefix = "firstValueIsNullInInputDBTest";
        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        JsonObject json = JsonObject.create().put("t_string1", "RRRR1").put("t_string2", "RRRR2").putNull("t_string3");
        bucket.insert(JsonDocument.create(generateDocId(idPrefix, 0), json));
        bucket.close();

        CouchbaseInputConfiguration inputConfiguration = getInputConfiguration();
        inputConfiguration.setSelectAction(SelectAction.N1QL);
        inputConfiguration
                .setQuery("SELECT `" + BUCKET_NAME + "`.* FROM `" + BUCKET_NAME + "` where meta().id like \"" + idPrefix + "%\"");
        executeJob(inputConfiguration);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertNotNull(res);

        Assertions.assertFalse(res.isEmpty());
        assertEquals(2, res.get(0).getSchema().getEntries().size());
    }

    @Test
    @DisplayName("Execution of customN1QL query")
    void n1qlQueryInputDBTest() {
        log.info("Test start: n1qlQueryInputDBTest");
        String idPrefix = "n1qlQueryInputDBTest";
        insertTestDataToDB(idPrefix);

        CouchbaseInputConfiguration configurationWithN1ql = getInputConfiguration();
        configurationWithN1ql.setSelectAction(SelectAction.N1QL);
        configurationWithN1ql.setQuery("SELECT `t_long_max`, `t_string`, `t_double_max` FROM `" + BUCKET_NAME
                + "` where meta().id like \"" + idPrefix + "%\"");
        executeJob(configurationWithN1ql);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);
        assertNotNull(res);

        assertEquals(2, res.size());
        assertEquals(3, res.get(0).getSchema().getEntries().size());
        assertEquals(3, res.get(1).getSchema().getEntries().size());
    }

    @Test
    @DisplayName("Check input binary data")
    void inputBinaryDocumentTest() {
        log.info("Test start: inputBinaryDocumentTest");
        String idPrefix = "inputBinaryDocumentTest";
        String docContent = "DocumentContent";

        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        for (int i = 0; i < 2; i++) {
            bucket.insert(
                    createBinaryDocument(generateDocId(idPrefix, i), (docContent + "_" + i).getBytes(StandardCharsets.UTF_8)));
        }
        bucket.close();

        CouchbaseInputConfiguration config = getInputConfiguration();
        config.getDataSet().setDocumentType(DocumentType.BINARY);
        executeJob(config);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("id").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("id"))).collect(Collectors.toList());
        assertEquals(2, data.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(generateDocId(idPrefix, i), data.get(i).getString("id"));
            assertArrayEquals((docContent + "_" + i).getBytes(StandardCharsets.UTF_8), data.get(i).getBytes("content"));
        }
    }

    @Test
    @DisplayName("Check input string data")
    void inputStringDocumentTest() {
        log.info("Test start: inputStringDocumentTest");
        String idPrefix = "inputStringDocumentTest";
        String docContent = "DocumentContent";

        Bucket bucket = couchbaseCluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        for (int i = 0; i < 2; i++) {
            bucket.insert(StringDocument.create(generateDocId(idPrefix, i), (docContent + "_" + i)));
        }
        bucket.close();

        CouchbaseInputConfiguration config = getInputConfiguration();
        config.getDataSet().setDocumentType(DocumentType.STRING);
        executeJob(config);

        final List<Record> res = componentsHandler.getCollectedData(Record.class);

        assertNotNull(res);
        List<Record> data = res.stream().filter(record -> record.getString("id").startsWith(idPrefix))
                .sorted(Comparator.comparing(r -> r.getString("id"))).collect(Collectors.toList());
        assertEquals(2, data.size());
        for (int i = 0; i < 2; i++) {
            assertEquals(generateDocId(idPrefix, i), data.get(i).getString("id"));
            assertEquals((docContent + "_" + i), data.get(i).getString("content"));
        }
    }

    @Test
    @DisplayName("Select document by ID")
    void oneDocumentInputDBTest() {
        insertTestDataToDB("oneDocumentInputDBTest");
        CouchbaseInputConfiguration configuration = getInputConfiguration();
        configuration.setSelectAction(SelectAction.ONE);
        configuration.setDocumentId("oneDocumentInputDBTest_1");
        executeJob(configuration);

        final List<Record> result = componentsHandler.getCollectedData(Record.class);

        assertEquals(1, result.size());
        assertOneRecord("2", result.get(0));
    }

    @Test
    @DisplayName("Select document by not exist ID")
    void oneNotExistDocumentInputDBTest() {
        CouchbaseInputConfiguration configuration = getInputConfiguration();
        configuration.setSelectAction(SelectAction.ONE);
        configuration.setDocumentId("notExistID");

        final List<Record> result = componentsHandler.getCollectedData(Record.class);
        assertEquals(0, result.size());
    }

    private void assertOneRecord(String id, Record record) {
        TestData testData = new TestData();
        assertEquals(testData.getColId() + id, record.getString("t_string"));
        assertEquals(testData.getColIntMin(), record.getInt("t_int_min"));
        assertEquals(testData.getColIntMax(), record.getInt("t_int_max"));
        assertEquals(testData.getColLongMin(), record.getLong("t_long_min"));
        assertEquals(testData.getColLongMax(), record.getLong("t_long_max"));
        assertEquals(testData.getColFloatMin(), record.getFloat("t_float_min"));
        assertEquals(testData.getColFloatMax(), record.getFloat("t_float_max"));
        assertEquals(testData.getColDoubleMin(), record.getDouble("t_double_min"));
        assertEquals(testData.getColDoubleMax(), record.getDouble("t_double_max"));
        assertEquals(testData.isColBoolean(), record.getBoolean("t_boolean"));
        assertEquals(testData.getColDateTime().toString(), record.getDateTime("t_datetime").toString());
        String arrayStrOriginal = "[" + testData.getColList().stream().map(s -> "\"" + s + "\"").collect(Collectors.joining(","))
                + "]";
        assertEquals(arrayStrOriginal, record.getString("t_array"));

    }

    private BinaryDocument createBinaryDocument(String id, byte[] bytes) {
        ByteBuf toWrite = Unpooled.copiedBuffer(bytes);
        return BinaryDocument.create(id, toWrite);
    }

    private CouchbaseInputConfiguration getInputConfiguration() {
        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setDatastore(couchbaseDataStore);
        couchbaseDataSet.setBucket(BUCKET_NAME);

        CouchbaseInputConfiguration configuration = new CouchbaseInputConfiguration();
        return configuration.setDataSet(couchbaseDataSet);
    }
}
