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

package org.talend.components.couchbase;

import com.couchbase.client.java.bucket.BucketType;
import com.couchbase.client.java.cluster.DefaultBucketSettings;
import com.couchbase.client.java.document.json.JsonObject;
import org.junit.jupiter.api.extension.Extension;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.SchemaImpl;
import org.testcontainers.couchbase.CouchbaseContainer;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import lombok.Data;

public abstract class CouchbaseUtilTest implements Extension {

    public static final String BUCKET_NAME = "student";

    public static final String BUCKET_PASSWORD = "secret";

    public static final int BUCKET_QUOTA = 100;

    public static final String CLUSTER_USERNAME = "student";

    public static final String CLUSTER_PASSWORD = "secret";

    public static final int DEFAULT_TIMEOUT_IN_SEC = 40;

    private static final List<String> ports = new ArrayList(
            Arrays.asList(new String[] { "8091:8091", "8092:8092", "8093:8093", "8094:8094", "11210:11210" }));

    public static final CouchbaseContainer COUCHBASE_CONTAINER;

    @Service
    private RecordBuilderFactory recordBuilderFactory;

    static {
        COUCHBASE_CONTAINER = new CouchbaseContainer().withClusterAdmin(CLUSTER_USERNAME, CLUSTER_PASSWORD)
                .withNewBucket(DefaultBucketSettings.builder().enableFlush(true).name(BUCKET_NAME).password(BUCKET_PASSWORD)
                        .quota(BUCKET_QUOTA).type(BucketType.COUCHBASE).build());
        COUCHBASE_CONTAINER.setPortBindings(ports);
        COUCHBASE_CONTAINER.start();
    }

    @Data
    public class TestData {

        private String col1 = "id";

        private int col2 = Integer.MAX_VALUE;

        private int col3 = Integer.MIN_VALUE;

        private long col4 = Long.MIN_VALUE;

        private long col5 = Long.MAX_VALUE;

        private float col6 = Float.MIN_VALUE;

        private float col7 = Float.MAX_VALUE;

        private double col8 = Double.MIN_VALUE;

        private double col9 = Double.MAX_VALUE;

        private boolean col10 = Boolean.TRUE;

        private ZonedDateTime col11 = ZonedDateTime.of(2018, 10, 30, 10, 30, 59, 0, ZoneId.of("UTC"));

        private List<String> col12 = new ArrayList<>(Arrays.asList("data1", "data2", "data3"));
    }

    public List<Record> createRecords() {
        TestData testData = new TestData();

        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        List<String> listTestData = new ArrayList<>();
        listTestData.add("one");
        listTestData.add("two");
        listTestData.add("three");

        List<Record> records = new ArrayList<>();

        SchemaImpl arrayInnerSchema = new SchemaImpl();
        arrayInnerSchema.setType(Schema.Type.STRING);

        Record record1 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), testData.col1 + "1")
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), testData.col2)
                .withInt(entryBuilder.withName("t_int_max").withType(Schema.Type.INT).build(), testData.col3)
                .withLong(entryBuilder.withName("t_long_min").withType(Schema.Type.LONG).build(), testData.col4)
                .withLong(entryBuilder.withName("t_long_max").withType(Schema.Type.LONG).build(), testData.col5)
                .withFloat(entryBuilder.withName("t_float_min").withType(Schema.Type.FLOAT).build(), testData.col6)
                .withFloat(entryBuilder.withName("t_float_max").withType(Schema.Type.FLOAT).build(), testData.col7)
                .withDouble(entryBuilder.withName("t_double_min").withType(Schema.Type.DOUBLE).build(), testData.col8)
                .withDouble(entryBuilder.withName("t_double_max").withType(Schema.Type.DOUBLE).build(), testData.col9)
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), testData.col10)
                .withDateTime(entryBuilder.withName("t_datetime").withType(Schema.Type.DATETIME).build(), testData.col11)
                .withArray(
                        entryBuilder.withName("t_array").withType(Schema.Type.ARRAY).withElementSchema(arrayInnerSchema).build(),
                        testData.col12)
                .build();
        Record record2 = recordBuilderFactory.newRecordBuilder()
                .withString(entryBuilder.withName("t_string").withType(Schema.Type.STRING).build(), testData.col1 + "2")
                .withInt(entryBuilder.withName("t_int_min").withType(Schema.Type.INT).build(), testData.col2)
                .withInt(entryBuilder.withName("t_int_max").withType(Schema.Type.INT).build(), testData.col3)
                .withLong(entryBuilder.withName("t_long_min").withType(Schema.Type.LONG).build(), testData.col4)
                .withLong(entryBuilder.withName("t_long_max").withType(Schema.Type.LONG).build(), testData.col5)
                .withFloat(entryBuilder.withName("t_float_min").withType(Schema.Type.FLOAT).build(), testData.col6)
                .withFloat(entryBuilder.withName("t_float_max").withType(Schema.Type.FLOAT).build(), testData.col7)
                .withDouble(entryBuilder.withName("t_double_min").withType(Schema.Type.DOUBLE).build(), testData.col8)
                .withDouble(entryBuilder.withName("t_double_max").withType(Schema.Type.DOUBLE).build(), testData.col9)
                .withBoolean(entryBuilder.withName("t_boolean").withType(Schema.Type.BOOLEAN).build(), testData.col10)
                .withDateTime(entryBuilder.withName("t_datetime").withType(Schema.Type.DATETIME).build(), testData.col11)
                .withArray(
                        entryBuilder.withName("t_array").withType(Schema.Type.ARRAY).withElementSchema(arrayInnerSchema).build(),
                        testData.col12)
                .build();
        records.add(record1);
        records.add(record2);

        return records;
    }

    public List<JsonObject> createJsonObjects() {
        TestData testData = new TestData();
        JsonObject json1 = JsonObject.create().put("t_string", testData.col1 + "1").put("t_int_min", testData.col2)
                .put("t_int_max", testData.col3).put("t_long_min", testData.col4).put("t_long_max", testData.col5)
                .put("t_float_min", testData.col6).put("t_float_max", testData.col7).put("t_double_min", testData.col8)
                .put("t_double_max", testData.col9).put("t_boolean", testData.col10).put("t_datetime", testData.col11.toString())
                .put("t_array", testData.col12);

        JsonObject json2 = JsonObject.create().put("t_string", testData.col1 + "2").put("t_int_min", testData.col2)
                .put("t_int_max", testData.col3).put("t_long_min", testData.col4).put("t_long_max", testData.col5)
                .put("t_float_min", testData.col6).put("t_float_max", testData.col7).put("t_double_min", testData.col8)
                .put("t_double_max", testData.col9).put("t_boolean", testData.col10).put("t_datetime", testData.col11.toString())
                .put("t_array", testData.col12);

        List<JsonObject> jsonObjects = new ArrayList<>();

        jsonObjects.add(json1);
        jsonObjects.add(json2);

        return jsonObjects;
    }
}
