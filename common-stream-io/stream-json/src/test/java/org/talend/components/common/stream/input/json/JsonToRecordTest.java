/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.input.json;

import java.io.StringReader;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.common.stream.output.json.RecordToJson;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonToRecordTest {

    final JsonObject jsonObject = Json.createObjectBuilder().add("Hello", "World")
            .add("array", Json.createArrayBuilder().add("First"))
            .add("arrayOfObject",
                    Json.createArrayBuilder().add(Json.createObjectBuilder().add("f1", "v1"))
                            .add(Json.createObjectBuilder().add("f1", "v2").add("f2", "v2f2").addNull("f3")))
            .add("arrayOfArray",
                    Json.createArrayBuilder().add(Json.createArrayBuilder().add(20.0).add(30.0).add(40.0))
                            .add(Json.createArrayBuilder().add(11.0).add(12.0).add(13.0)))
            .add("subRecord", Json.createObjectBuilder().add("field_1", "val1").add("field_2", "val2")).build();

    private JsonToRecord toRecord;

    private JsonToRecord toRecordDoubleOption;

    @BeforeAll
    static void initLog() {
        System.setProperty("org.slf4j.simpleLogger.log.org.talend.components.common.stream", "debug");
    }

    @BeforeEach
    void start() {
        start(false);
    }

    void start(final boolean forceDouble) {
        final RecordBuilderFactory recordBuilderFactory = new RecordBuilderFactoryImpl("test");
        this.toRecord = new JsonToRecord(recordBuilderFactory, forceDouble);
        toRecordDoubleOption = new JsonToRecord(recordBuilderFactory, true);
    }

    @Test
    void toRecord() {
        final Record record = toRecord.toRecord(this.jsonObject);
        Assertions.assertNotNull(record);

        final RecordToJson toJson = new RecordToJson();
        final JsonObject jsonResult = toJson.fromRecord(record);
        Assertions.assertNotNull(jsonResult);

        // object equals except for 'null' value
        Assertions.assertEquals(this.jsonObject.getString("Hello"), jsonResult.getString("Hello"));
        Assertions.assertEquals(this.jsonObject.getJsonArray("array"), jsonResult.getJsonArray("array"));
        Assertions.assertEquals(this.jsonObject.getJsonArray("arrayOfArray"), jsonResult.getJsonArray("arrayOfArray"));
        Assertions.assertEquals(this.jsonObject.getJsonObject("subRecord"), jsonResult.getJsonObject("subRecord"));

        final JsonArray array = this.jsonObject.getJsonArray("arrayOfObject");
        final JsonArray resultArray = jsonResult.getJsonArray("arrayOfObject");

        Assertions.assertEquals(array.get(0).asJsonObject().getString("f1"), resultArray.get(0).asJsonObject().getString("f1"));
        Assertions.assertEquals(array.get(1).asJsonObject().getString("f1"), resultArray.get(1).asJsonObject().getString("f1"));
        Assertions.assertEquals(array.get(1).asJsonObject().getString("f2"), resultArray.get(1).asJsonObject().getString("f2"));
    }

    @Test
    void toRecordWithDollarChar() {
        JsonObject jsonWithDollarChar = getJsonObject(
                "{\"_id\": {\"$oid\": \"5e66158f6eddd6049f309ddb\"}, \"date\": {\"$date\": 1543622400000}, \"item\": \"Cake - Chocolate\", \"quantity\": 2.0, \"amount\": {\"$numberDecimal\": \"60\"}}");
        final Record record = toRecord.toRecord(jsonWithDollarChar);
        Assertions.assertNotNull(record);
        Assertions.assertNotNull(record.getRecord("_id").getString("oid"));
    }

    @Test
    void toRecordWithHyphen() {
        JsonObject jsonWithDollarChar = getJsonObject("{\"_id\": {\"Content-Type\" : \"text/plain\"}}");
        final Record record = toRecord.toRecord(jsonWithDollarChar);
        Assertions.assertNotNull(record);
        Assertions.assertNotNull("text/plain", record.getRecord("_id").getString("Content_Type"));
    }

    @Test
    void numberToRecord() {
        String source = "{\n \"aNumber\" : 7,\n \"aaa\" : [1, 2, 3.0]\n}";
        JsonObject json = getJsonObject(source);
        final Record record = toRecord.toRecord(json);

        Assertions.assertNotNull(record);
        final Entry aaaEntry = findEntry(record.getSchema(), "aaa");
        Assertions.assertNotNull(aaaEntry);

        Assertions.assertEquals(Schema.Type.ARRAY, aaaEntry.getType());
        Assertions.assertEquals(Schema.Type.LONG, aaaEntry.getElementSchema().getType());

        final Entry aNumberEntry = findEntry(record.getSchema(), "aNumber");
        Assertions.assertEquals(Schema.Type.LONG, aNumberEntry.getType());

        RecordToJson toJson = new RecordToJson();
        final JsonObject jsonObject = toJson.fromRecord(record);
        Assertions.assertNotNull(jsonObject);

        final Record recordDouble = toRecordDoubleOption.toRecord(json);
        Assertions.assertNotNull(recordDouble);
        final Entry aaaEntryDouble = findEntry(recordDouble.getSchema(), "aaa");
        Assertions.assertNotNull(aaaEntry);

        Assertions.assertEquals(Schema.Type.ARRAY, aaaEntryDouble.getType());
        Assertions.assertEquals(Schema.Type.DOUBLE, aaaEntryDouble.getElementSchema().getType());

        final Entry aNumberEntryDouble = findEntry(recordDouble.getSchema(), "aNumber");
        Assertions.assertEquals(Schema.Type.DOUBLE, aNumberEntryDouble.getType());
    }

    @ParameterizedTest
    @ValueSource(booleans = { true, false })
    void fieldAreNullable(final boolean forceDouble) {
        start(forceDouble);

        String source = "{\"a_string\" : \"string1\", \"a_long\" : 123, \"a_double\" : 123.123, \"a_boolean\" : true, \"an_object\" : {\"att_a\" : \"aaa\", \"att_b\" : \"bbb\"}, \"an_array\" : [\"aaa\", \"bbb\", \"ccc\"]}";
        JsonObject json = getJsonObject(source);
        final Record record = toRecord.toRecord(json);

        record.getSchema().getEntries().stream().forEach(e -> {
            Assertions.assertTrue(e.isNullable(), e.getName() + " of type " + e.getType() + " should be nullable.");
        });
    }

    private Entry findEntry(Schema schema, String entryName) {
        return schema.getEntries().stream().filter((Entry e) -> entryName.equals(e.getName())).findFirst().orElse(null);
    }

    private JsonObject getJsonObject(String content) {
        try (JsonReader reader = Json.createReader(new StringReader(content))) {
            return reader.readObject();
        }
    }
}