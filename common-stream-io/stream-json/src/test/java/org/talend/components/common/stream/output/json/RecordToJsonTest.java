package org.talend.components.common.stream.output.json;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Iterator;
import java.util.List;

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.test.records.DatasetGenerator;
import org.talend.components.common.test.records.DatasetGenerator.DataSet;
import org.talend.components.common.test.records.ExpectedValueBuilder;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class RecordToJsonTest {
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
        final ExpectedValueBuilder<JsonObject> valueBuilder = new JsonExpected();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        DatasetGenerator<JsonObject> generator = new DatasetGenerator<>(factory, valueBuilder);
        return generator.generate(40);
    }

    private static void checkList(final JsonObject realValues, final JsonObject expectedValue) {

        assertEquals(realValues.size(), expectedValue.size(), expectedValue.toString());
        for (int i = 0; i < realValues.size(); i++) {
            assertEquals(realValues.get(i), expectedValue.get(i), "Error on field " + i);
        }
    }
}