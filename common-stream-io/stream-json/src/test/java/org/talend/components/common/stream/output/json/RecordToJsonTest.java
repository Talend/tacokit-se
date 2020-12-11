package org.talend.components.common.stream.output.json;

import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class RecordToJsonTest {
    final RecordToJson toJson = new RecordToJson();

    @Test
    void convert() {
        final JsonObject jsonObject = toJson.fromRecord(null);
        Assertions.assertNull(jsonObject);
    }



}