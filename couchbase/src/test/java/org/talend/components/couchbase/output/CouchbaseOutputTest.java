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
package org.talend.components.couchbase.output;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.talend.components.couchbase.CouchbaseUtilTest;
import org.talend.components.couchbase.dataset.CouchbaseDataSet;
import org.talend.components.couchbase.datastore.CouchbaseDataStore;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.query.N1qlQuery;
import com.couchbase.client.java.query.N1qlQueryResult;
import com.couchbase.client.java.query.N1qlQueryRow;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.talend.sdk.component.junit.SimpleFactory.configurationByExample;

@WithComponents("org.talend.components.couchbase")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@DisplayName("Testing of CouchbaseOutput component")
public class CouchbaseOutputTest extends CouchbaseUtilTest {

    private List<Record> records;

    private List<JsonDocument> retrieveDataFromDatabase() {
        CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC * 1000)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getContainerIpAddress());
        Bucket bucket = cluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);

        bucket.bucketManager().createN1qlPrimaryIndex(true, false);

        N1qlQueryResult n1qlQueryResult = bucket.query(N1qlQuery.simple("SELECT META(" + BUCKET_NAME + ").id FROM " + BUCKET_NAME
                + " WHERE key3 IS MISSING ORDER BY " + "META(" + BUCKET_NAME + ").id"));
        List<JsonDocument> resultList = n1qlQueryResult.allRows().stream().map(index -> index.value().get("id"))
                .map(Object::toString).map(index -> bucket.get(index)).collect(Collectors.toList());

        bucket.close();
        cluster.disconnect();
        environment.shutdown();
        return resultList;
    }

    @BeforeEach
    void createTestRecords() {
        records = super.createRecords();
        componentsHandler.setInputData(records);
        executeJob();
    }

    void executeJob(String config) {
        Job.components().component("Couchbase_Output", "Couchbase://Output?" + config).component("emitter", "test://emitter")
                .connections().from("emitter").to("Couchbase_Output").build().run();
    }

    void executeJob() {
        executeJob(configurationByExample().forInstance(getOutputConfiguration()).configured().toQueryString());
    }

    @Test
    @DisplayName("Check amount of total records from retrieved data")
    void sizeOfRetrievedCouchbaseInsertTest() {
        assertEquals(2, retrieveDataFromDatabase().size());
    }

    @Test
    @DisplayName("Check fields from retrieved data")
    void checkDataCouchbaseInsertTest() {
        List<JsonDocument> resultList = retrieveDataFromDatabase();
        TestData testData = new TestData();

        assertEquals(new Integer(testData.getCol2()), resultList.get(0).content().getInt("t_int_min"));
        assertEquals(new Integer(testData.getCol3()), resultList.get(0).content().getInt("t_int_max"));
        assertEquals(new Long(testData.getCol4()), resultList.get(0).content().getLong("t_long_min"));
        assertEquals(new Long(testData.getCol5()), resultList.get(0).content().getLong("t_long_max"));
        assertEquals(testData.getCol6(), resultList.get(0).content().getDouble("t_float_min"), 1E35);
        assertEquals(testData.getCol7(), resultList.get(0).content().getDouble("t_float_max"), 1E35);
        assertEquals(testData.getCol8(), resultList.get(0).content().getDouble("t_double_min"), 1);
        assertEquals(testData.getCol9(), resultList.get(0).content().getDouble("t_double_max"), 1);
        assertEquals(testData.isCol10(), resultList.get(0).content().getBoolean("t_boolean"));
        assertEquals(testData.getCol11().toString(), resultList.get(0).content().getString("t_datetime"));
        assertArrayEquals(testData.getCol12().toArray(), resultList.get(0).content().getArray("t_array").toList().toArray());

        assertEquals(2, resultList.size());
    }

    private CouchbaseOutputConfiguration getOutputConfiguration() {
        CouchbaseDataStore couchbaseDataStore = new CouchbaseDataStore();
        couchbaseDataStore.setBootstrapNodes(COUCHBASE_CONTAINER.getContainerIpAddress());
        couchbaseDataStore.setUsername(CLUSTER_USERNAME);
        couchbaseDataStore.setPassword(CLUSTER_PASSWORD);
        couchbaseDataStore.setConnectTimeout(DEFAULT_TIMEOUT_IN_SEC);

        CouchbaseDataSet couchbaseDataSet = new CouchbaseDataSet();
        couchbaseDataSet.setBucket(BUCKET_NAME);
        couchbaseDataSet.setDatastore(couchbaseDataStore);

        CouchbaseOutputConfiguration configuration = new CouchbaseOutputConfiguration();
        configuration.setIdFieldName("t_string");
        configuration.setDataSet(couchbaseDataSet);
        return configuration;
    }

    private List<Record> createRecordsForN1QL() {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        return Stream.iterate(1, op -> op + 1).limit(5)
                .map(idx -> recordBuilderFactory.newRecordBuilder()
                        .withString(entryBuilder.withName("docId").withType(Schema.Type.STRING).build(), "DOC_PK_" + idx)
                        .withString(entryBuilder.withName("key1").withType(Schema.Type.STRING).build(), "ZzZ" + idx)
                        .withString(entryBuilder.withName("key2").withType(Schema.Type.STRING).build(), "ZzTop" + idx)
                        .withString(entryBuilder.withName("key3").withType(Schema.Type.STRING).build(), "KEY_3")
                        .withInt(entryBuilder.withName("count").withType(Type.INT).build(), idx).build())
                .collect(Collectors.toList());
    }

    private List<N1qlQueryRow> retrieveN1QLQueryParamDataFromDatabase(String testingKey) {
        CouchbaseEnvironment environment = new DefaultCouchbaseEnvironment.Builder().connectTimeout(DEFAULT_TIMEOUT_IN_SEC * 1000)
                .build();
        Cluster cluster = CouchbaseCluster.create(environment, COUCHBASE_CONTAINER.getContainerIpAddress());
        Bucket bucket = cluster.openBucket(BUCKET_NAME, BUCKET_PASSWORD);
        bucket.bucketManager().createN1qlPrimaryIndex(true, false);
        N1qlQueryResult n1qlQueryResult = bucket.query(N1qlQuery.simple("SELECT META(" + BUCKET_NAME
                + ").id, key1, key2, key3, count FROM " + BUCKET_NAME + " WHERE key3='" + testingKey + "'"));
        List<N1qlQueryRow> resultList = n1qlQueryResult.allRows();
        bucket.close();
        cluster.disconnect();
        environment.shutdown();
        return resultList;
    }

    @Test
    @DisplayName("Simple N1QL query with no parameters")
    void executeSimpleN1QLQueryWithNoParameters() {
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);
        String qry = String.format(
                "UPSERT INTO `%s` (KEY, VALUE) VALUES (\"PK000\", {\"key1\": \"masterkey1\", \"key2\": \"masterkey2\", \"key3\": \"masterkey3\", \"count\": 19})",
                BUCKET_NAME);
        configuration.setQuery(qry);
        String cfg = configurationByExample().forInstance(configuration).configured().toQueryString();
        componentsHandler.setInputData(createRecordsForN1QL());
        executeJob(cfg);
        List<N1qlQueryRow> results = retrieveN1QLQueryParamDataFromDatabase("masterkey3");
        assertEquals(1, results.size());
        N1qlQueryRow result = results.get(0);
        assertEquals("PK000", result.value().getString("id"));
        assertEquals("masterkey1", result.value().getString("key1"));
        assertEquals("masterkey2", result.value().getString("key2"));
        assertEquals("masterkey3", result.value().getString("key3"));
        assertEquals(19, result.value().getInt("count"));
    }

    @Test
    @DisplayName("N1QL query with parameters")
    void executeSimpleN1QLQueryWithParameters() {
        CouchbaseOutputConfiguration configuration = getOutputConfiguration();
        configuration.setUseN1QLQuery(true);
        String qry = String.format(
                "INSERT INTO `%s` (KEY, VALUE) VALUES ($id, {\"key1\": $k1, \"key2\": $k2, " + "\"key3\": $k3, \"count\": $cn})",
                BUCKET_NAME);
        configuration.setQuery(qry);
        List<N1QLQueryParameter> params = new ArrayList<>();
        params.add(new N1QLQueryParameter("id", "docId"));
        params.add(new N1QLQueryParameter("k1", "key1"));
        params.add(new N1QLQueryParameter("k2", "key2"));
        params.add(new N1QLQueryParameter("k3", "key3"));
        params.add(new N1QLQueryParameter("cn", "count"));
        configuration.setQueryParams(params);
        String cfg = configurationByExample().forInstance(configuration).configured().toQueryString();
        componentsHandler.setInputData(createRecordsForN1QL());
        executeJob(cfg);
        List<N1qlQueryRow> results = retrieveN1QLQueryParamDataFromDatabase("KEY_3");
        assertEquals(5, results.size());
        Stream.iterate(1, o -> o + 1).limit(5).forEach(idx -> {
            N1qlQueryRow result = results.get(idx - 1);
            assertEquals("DOC_PK_" + idx, result.value().getString("id"));
            assertEquals("ZzZ" + idx, result.value().getString("key1"));
            assertEquals("ZzTop" + idx, result.value().getString("key2"));
            assertEquals("KEY_3", result.value().getString("key3"));
            assertEquals(idx, result.value().getInt("count"));
        });
    }

    public List<Record> createPartialUpdateRecords() {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        List<Record> records = new ArrayList<>();
        Record record1 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), "id1")
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), 1971)
                .withString(entryBuilder.withName("extra_content").withType(Schema.Type.STRING).build(), "path new").build();
        Record record2 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), "id2")
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), Boolean.FALSE)
                .withString(entryBuilder.withName("extra_content2").withType(Schema.Type.STRING).build(), "path zap").build();
        records.add(record1);
        records.add(record2);

        return records;
    }

    @Test
    @DisplayName("Document partial update")
    void partialUpdate() {
        CouchbaseOutputConfiguration config = getOutputConfiguration();
        config.setPartialUpdate(true);
        String cfg = configurationByExample().forInstance(config).configured().toQueryString();
        componentsHandler.setInputData(createPartialUpdateRecords());
        executeJob(cfg);
        //
        List<JsonDocument> resultList = retrieveDataFromDatabase();
        assertEquals(2, resultList.size());
        TestData testData = new TestData();
        Stream.iterate(0, o -> o + 1).limit(2).forEach(idx -> {
            // untouched properties
            assertEquals(new Integer(testData.getCol3()), resultList.get(idx).content().getInt("t_int_max"));
            assertEquals(new Long(testData.getCol4()), resultList.get(idx).content().getLong("t_long_min"));
            assertEquals(new Long(testData.getCol5()), resultList.get(idx).content().getLong("t_long_max"));
            assertEquals(testData.getCol6(), resultList.get(idx).content().getDouble("t_float_min"), 1E35);
            assertEquals(testData.getCol7(), resultList.get(idx).content().getDouble("t_float_max"), 1E35);
            assertEquals(testData.getCol8(), resultList.get(idx).content().getDouble("t_double_min"), 1);
            assertEquals(testData.getCol9(), resultList.get(idx).content().getDouble("t_double_max"), 1);
            assertEquals(testData.getCol11().toString(), resultList.get(idx).content().getString("t_datetime"));
            assertArrayEquals(testData.getCol12().toArray(),
                    resultList.get(idx).content().getArray("t_array").toList().toArray());
            // upserted proterties
            if (idx == 0) {
                assertEquals(1971, resultList.get(idx).content().getInt("t_int_min"));
                assertEquals(testData.isCol10(), resultList.get(idx).content().getBoolean("t_boolean"));
                assertEquals("path new", resultList.get(idx).content().getString("extra_content"));
                assertNull(resultList.get(idx).content().getString("extra_content2"));
            } else {
                assertEquals(new Integer(testData.getCol2()), resultList.get(idx).content().getInt("t_int_min"));
                assertEquals(Boolean.FALSE, resultList.get(idx).content().getBoolean("t_boolean"));
                assertEquals("path zap", resultList.get(idx).content().getString("extra_content2"));
                assertNull(resultList.get(idx).content().getString("extra_content"));
            }
        });
    }
}
