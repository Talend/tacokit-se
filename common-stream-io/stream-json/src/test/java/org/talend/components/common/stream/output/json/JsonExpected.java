package org.talend.components.common.stream.output.json;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.json.JsonObject;
import javax.json.JsonValue;

import org.junit.jupiter.api.Assertions;
import org.talend.components.common.test.records.ExpectedValueBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;

import lombok.RequiredArgsConstructor;

public class JsonExpected implements ExpectedValueBuilder<JsonObject> {

    final List<Consumer<JsonObject>> verifiers = new ArrayList<>();

    @Override
    public void startRecord(int id) {
        this.verifiers.clear();
    }

    @Override
    public void addField(int id, Entry field, Object value) {
        final Consumer<JsonObject> verifier = (JsonObject obj) -> this.checkContains(obj, field.getName(), value);
        this.verifiers.add(verifier);
    }

    @Override
    public Consumer<JsonObject> endRecord(int id, Record record) {
        final List<Consumer<JsonObject>> copy = new ArrayList<>(this.verifiers.size());
        copy.addAll(this.verifiers);
        return new JsonChecker(copy);
    }

    @RequiredArgsConstructor
    static class JsonChecker implements Consumer<JsonObject> {

        final List<Consumer<JsonObject>> verifiers;

        @Override
        public void accept(final JsonObject realJson) {
            Assertions.assertNotNull(realJson);
            verifiers.forEach((Consumer<JsonObject> verif) -> verif.accept(realJson));
        }
    }

    private void checkContains(JsonObject obj, String name, Object expectedValue) {
        if (expectedValue == null) {
            Assertions.assertTrue(obj.isNull(name), name + " should be null");
        }
        else if (expectedValue instanceof String) {
            final String realValue = obj.getString(name);
            Assertions.assertEquals(expectedValue, realValue);
        }
        else if (expectedValue instanceof Integer) {
            final int value = obj.getInt(name);
            Assertions.assertEquals(expectedValue, value);
        }
        else if (expectedValue instanceof Long) {
            final long value = obj.getJsonNumber(name).longValue();
            Assertions.assertEquals(expectedValue, value);
        }
        else if (expectedValue instanceof Record) {
            final JsonObject value = obj.getJsonObject(name);
            Assertions.assertNotNull(value);
        }
    }
}
