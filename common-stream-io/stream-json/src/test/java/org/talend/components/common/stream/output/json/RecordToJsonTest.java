package org.talend.components.common.stream.output.json;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class RecordToJsonTest {

    @Test
    void fromRecord() {
        final RecordToJson toJson = new RecordToJson();

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final Record record1 = factory.newRecordBuilder()
                .withDateTime("fieldDateTime", ZonedDateTime.of(2020, 10, 10, 23, 25, 10, 0, ZoneId.systemDefault()))
                .build();

        final JsonObject jsonObject1 = toJson.fromRecord(record1);
        Assertions.assertNotNull(jsonObject1);
        final String fieldDateTime = jsonObject1.getString("fieldDateTime");
        Assertions.assertNotNull(fieldDateTime);
        Assertions.assertTrue(fieldDateTime.startsWith("2020-10-10"));

        final Record record2 = factory.newRecordBuilder()
                .withDateTime("fieldDateTime", (ZonedDateTime) null)
                .build();

        final JsonObject jsonObject2 = toJson.fromRecord(record2);
        Assertions.assertNotNull(jsonObject2);
        Assertions.assertTrue(jsonObject2.isNull("fieldDateTime"));
    }
}