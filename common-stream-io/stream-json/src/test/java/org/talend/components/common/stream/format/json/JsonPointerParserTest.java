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
package org.talend.components.common.stream.format.json;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.Iterator;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;
import javax.json.stream.JsonParser;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class JsonPointerParserTest {

    @Test
    void valuesSimpleArray() {
        JsonParser parser = Json.createParser(new StringReader("[1, 2, 3]"));
        final JsonPointerParser pointer = JsonPointerParser.of("/");

        Iterator<JsonValue> values = pointer.values(parser);
        for (int i = 0; i < 6; i++) {
            Assertions.assertTrue(values.hasNext()); // must be re-entrant.
        }
        for (int val = 1; val <= 3; val++) {
            Assertions.assertTrue(values.hasNext());
            JsonValue v = values.next();
            Assertions.assertNotNull(v);
            Assertions.assertSame(v.getValueType(), ValueType.NUMBER);
            Assertions.assertEquals(val, ((JsonNumber) v).intValue());
        }

        Assertions.assertFalse(values.hasNext());
    }

    @Test
    void valuesInnerArray() throws IOException {
        try (final JsonParser parser = createParser("complexObject.json")) {
            final JsonPointerParser pointer = JsonPointerParser.of("/a/b");

            Iterator<JsonValue> values = pointer.values(parser);
            for (int i = 0; i < 6; i++) {
                Assertions.assertTrue(values.hasNext()); // must be re-entrant.
            }
            JsonValue vOne = values.next();
            Assertions.assertSame(vOne.getValueType(), ValueType.NUMBER);
            Assertions.assertEquals(1, ((JsonNumber) vOne).intValue());

            Assertions.assertTrue(values.hasNext());
            JsonValue vSec = values.next();
            Assertions.assertSame(vSec.getValueType(), JsonValue.ValueType.OBJECT);
            Assertions.assertEquals("obj", vSec.asJsonObject().getString("sub"));
            final JsonArray field2 = vSec.asJsonObject().getJsonArray("field2");
            Assertions.assertEquals(1, field2.getInt(0));
            Assertions.assertEquals(2, field2.getInt(1));
            Assertions.assertEquals(3, field2.getInt(2));

            Assertions.assertTrue(values.hasNext());
            JsonValue v3 = values.next();
            Assertions.assertSame(v3.getValueType(), ValueType.ARRAY);
            Assertions.assertEquals("a", v3.asJsonArray().getString(0));

            Assertions.assertFalse(values.hasNext());
        }
    }

    @Test
    void valuesInnerObject() throws IOException {
        try (final JsonParser parser = createParser("complexObject.json")) {
            final JsonPointerParser pointer = JsonPointerParser.of("/a/subobj");
            Iterator<JsonValue> values = pointer.values(parser);
            for (int i = 0; i < 6; i++) {
                Assertions.assertTrue(values.hasNext()); // must be re-entrant.
            }

            Assertions.assertTrue(values.hasNext());
            JsonValue vObject = values.next();
            Assertions.assertSame(vObject.getValueType(), JsonValue.ValueType.OBJECT);
            Assertions.assertEquals("v1", vObject.asJsonObject().getString("f1"));

            Assertions.assertFalse(values.hasNext());
            Assertions.assertFalse(values.hasNext());
        }
    }

    @Test
    void valuesUnexist() throws IOException {
        try (final JsonParser parser = createParser("complexObject.json")) {
            final JsonPointerParser pointer = JsonPointerParser.of("/a/unknown");
            final Iterator<JsonValue> values = pointer.values(parser);
            Assertions.assertFalse(values.hasNext());
        }
    }

    private JsonParser createParser(String file) {
        final InputStream json = Thread.currentThread().getContextClassLoader().getResourceAsStream(file);
        final Reader reader = new InputStreamReader(json);

        return Json.createParser(reader);
    }
}