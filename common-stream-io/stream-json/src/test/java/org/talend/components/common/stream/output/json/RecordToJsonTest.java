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
package org.talend.components.common.stream.output.json;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Iterator;

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.test.records.AssertionsBuilder;
import org.talend.components.common.test.records.DatasetGenerator;
import org.talend.components.common.test.records.DatasetGenerator.DataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class RecordToJsonTest {

    @Test
    void fromRecord() {
        final RecordToJson toJson = new RecordToJson();

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final Record record1 = factory.newRecordBuilder()
                .withDateTime("fieldDateTime", ZonedDateTime.of(2020, 10, 10, 23, 25, 10, 0, ZoneId.systemDefault())).build();

        final JsonObject jsonObject1 = toJson.fromRecord(record1);
        Assertions.assertNotNull(jsonObject1);
        final String fieldDateTime = jsonObject1.getString("fieldDateTime");
        Assertions.assertNotNull(fieldDateTime);
        Assertions.assertTrue(fieldDateTime.startsWith("2020-10-10"));

        final Record record2 = factory.newRecordBuilder().withDateTime("fieldDateTime", (ZonedDateTime) null).build();

        final JsonObject jsonObject2 = toJson.fromRecord(record2);
        Assertions.assertNotNull(jsonObject2);
        Assertions.assertTrue(jsonObject2.isNull("fieldDateTime"));
    }

    final RecordToJson toJson = new RecordToJson();

    @Test
    void convert() {
        final JsonObject jsonObject = toJson.fromRecord(null);
        Assertions.assertNull(jsonObject);
    }

    @ParameterizedTest
    @MethodSource("testDataJson")
    void testRecordsLine(DataSet<JsonObject> ds) {
        final JsonObject jsonObject = toJson.fromRecord(ds.getRecord());
        ds.check(jsonObject);
    }

    private static Iterator<DataSet<JsonObject>> testDataJson() {
        final AssertionsBuilder<JsonObject> valueBuilder = new JsonExpected();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        DatasetGenerator<JsonObject> generator = new DatasetGenerator<>(factory, valueBuilder);
        return generator.generate(40);
    }

}