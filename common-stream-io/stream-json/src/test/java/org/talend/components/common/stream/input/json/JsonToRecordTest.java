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
package org.talend.components.common.stream.input.json;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonToRecordTest {

    final JsonObject jsonObject = Json.createObjectBuilder().add("Hello", "World")
            .add("array", Json.createArrayBuilder().add("First"))
            .add("arrayOfObject",
                    Json.createArrayBuilder().add(Json.createObjectBuilder().add("f1", "v1"))
                            .add(Json.createObjectBuilder().add("f1", "v2").add("f2", "v2f2").addNull("f3")))
            .add("arrayOfArray",
                    Json.createArrayBuilder().add(Json.createArrayBuilder().add(20).add(30).add(40))
                            .add(Json.createArrayBuilder().add(11).add(12).add(13)))
            .add("subRecord", Json.createObjectBuilder().add("field_1", "val1").add("field_2", "val2")).build();

    private JsonToRecord toRecord;

    @BeforeEach
    void start() {
        final RecordBuilderFactory recordBuilderFactory = new RecordBuilderFactoryImpl("test");
        this.toRecord = new JsonToRecord(recordBuilderFactory);
    }

    @Test
    void inferSchema() {
        final Schema schema = this.toRecord.inferSchema(this.jsonObject);
        Assertions.assertNotNull(schema);
        Assertions.assertSame(schema.getType(), Schema.Type.RECORD);
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
}