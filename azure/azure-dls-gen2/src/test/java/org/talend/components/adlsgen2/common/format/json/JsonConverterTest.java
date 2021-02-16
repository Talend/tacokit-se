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
package org.talend.components.adlsgen2.common.format.json;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.time.ZonedDateTime;

import javax.json.Json;
import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.components.common.converters.JsonConverter;
import org.talend.components.common.formats.JSONFormatOptions;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.junit5.WithComponents;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@WithComponents("org.talend.components.adlsgen2")
class JsonConverterTest extends AdlsGen2TestBase {

    private JsonConverter converter;

    private JsonObject json;

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();

        converter = JsonConverter.of(recordBuilderFactory, jsonBuilderFactory, new JSONFormatOptions());
        String sample = getClass().getResource("/common/format/json/sample.json").getPath();
        json = Json.createParser(new InputStreamReader(new FileInputStream(sample))).getObject();
    }

    @Test
    void inferSchema() {
        Schema s = converter.inferSchema(json);
        assertNotNull(s);
    }

    @Test
    void fromRecord() {
        JsonObject from = converter.fromRecord(versatileRecord);
        assertEquals("Bonjour", from.getString("string1"));
        assertEquals("Olà", from.getString("string2"));
        assertEquals(71, from.getJsonNumber("int").intValue());
        assertEquals(1971, from.getJsonNumber("long").longValue());
        assertEquals(20.5f, from.getJsonNumber("float").doubleValue());
        assertEquals(20.5, from.getJsonNumber("double").doubleValue());
        assertTrue(from.getBoolean("boolean"));
        assertEquals("2019-04-22T00:00Z[UTC]", from.getString("datetime"));
        from = converter.fromRecord(complexRecord);
        assertEquals("ComplexR", from.getString("name"));
        assertEquals("Bonjour", from.getJsonObject("record").getString("string1"));
        assertEquals("Olà", from.getJsonObject("record").getString("string2"));
        assertEquals(71, from.getJsonObject("record").getJsonNumber("int").intValue());
        assertEquals(1971, from.getJsonObject("record").getJsonNumber("long").longValue());
        assertEquals(20.5f, from.getJsonObject("record").getJsonNumber("float").doubleValue());
        assertEquals(20.5, from.getJsonObject("record").getJsonNumber("double").doubleValue());
        assertTrue(from.getJsonObject("record").getBoolean("boolean"));
        assertEquals(now.toInstant().toEpochMilli(), ZonedDateTime.parse(from.getString("now")).toInstant().toEpochMilli());
    }

    @Test
    void toRecord() {
        Record record = converter.toRecord(json);
        assertNotNull(record);
        assertEquals("Bonjour", record.getString("string1"));
        assertEquals("Olà", record.getString("string2"));
        assertEquals(1971, record.getLong("long"));
        assertEquals(19.71, record.getDouble("double"));
        assertFalse(record.getBoolean("false"));
        assertTrue(record.getBoolean("true"));
        assertEquals("prop1", record.getRecord("object").getString("prop1"));
        assertEquals(2, record.getRecord("object").getLong("prop2"));
        assertThat(record.getArray(Long.class, "intary"), contains(12l, 212l, 4343l, 545l));
        assertEquals(2, record.getArray(Record.class, "ary").size());
        record.getArray(Record.class, "ary").stream().map(record1 -> {
            assertTrue(record1.getString("id").contains("string id"));
            assertTrue(record1.getLong("cost") % 1024 == 0);
            return null;
        });
    }

}
