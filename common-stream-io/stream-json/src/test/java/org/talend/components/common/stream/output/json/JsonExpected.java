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

import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.junit.jupiter.api.Assertions;
import org.talend.components.common.test.records.AssertionsBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.RequiredArgsConstructor;

public class JsonExpected implements AssertionsBuilder<JsonObject> {

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
        } else if (expectedValue instanceof String) {
            final String realValue = obj.getString(name);
            Assertions.assertEquals(expectedValue, realValue);
        } else if (expectedValue instanceof Integer) {
            final int value = obj.getInt(name);
            Assertions.assertEquals(expectedValue, value);
        } else if (expectedValue instanceof Long) {
            final long value = obj.getJsonNumber(name).longValue();
            Assertions.assertEquals(expectedValue, value);
        } else if (expectedValue instanceof Record) {
            final JsonObject value = obj.getJsonObject(name);
            Assertions.assertNotNull(value);
        } else if (expectedValue instanceof Boolean) {
            final boolean value = obj.getBoolean(name);
            Assertions.assertEquals(expectedValue, value);
        } else if (expectedValue instanceof Float) {
            final float value = obj.getJsonNumber(name).numberValue().floatValue();
            Assertions.assertEquals(expectedValue, value, "Error for float on " + name + " field");
        } else if (expectedValue instanceof Double) {
            final double value = obj.getJsonNumber(name).doubleValue();
            Assertions.assertEquals(expectedValue, value, "Error for double on " + name + " field");
        } else if (expectedValue instanceof ZonedDateTime) {
            final String value = obj.getString(name);
            Assertions.assertEquals(expectedValue.toString(), value, "Error on date time");
        } else if (expectedValue instanceof byte[]) {
            final String value = obj.getString(name);
            Assertions.assertArrayEquals((byte[]) expectedValue, value.getBytes());
        } else if (expectedValue instanceof List) {
            final JsonArray array = obj.getJsonArray(name);
            final List<?> expectedList = (List<?>) expectedValue;
            Assertions.assertEquals(expectedList.size(), array.size(), "Error array size");
        }
    }
}
