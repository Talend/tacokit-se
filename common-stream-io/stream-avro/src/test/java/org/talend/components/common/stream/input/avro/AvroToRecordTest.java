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
package org.talend.components.common.stream.input.avro;

import static java.util.stream.Collectors.toList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Stream;

import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class AvroToRecordTest {

    private RecordBuilderFactory recordBuilderFactory;

    private GenericRecord avro;

    @BeforeEach
    protected void setUp() throws Exception {
        recordBuilderFactory = new RecordBuilderFactoryImpl("test");

        final org.apache.avro.Schema schema = SchemaBuilder.builder().record("sample").fields() //
                .name("string").type().stringType().noDefault() //
                .name("int").type().intType().noDefault() //
                .name("long").type().longType().noDefault() //
                .name("double").type().doubleType().noDefault() //
                .name("boolean").type().booleanType().noDefault() //
                .name("array").type().array().items().intType().noDefault() // Array of int
                .name("object").type().record("obj") // sub obj
                .fields().name("f1").type().intType().noDefault() //
                .name("f2").type().stringType().noDefault().endRecord().noDefault().endRecord();

        avro = new GenericData.Record(schema);
        avro.put("string", "a string sample");
        avro.put("int", 710);
        avro.put("long", 710L);
        avro.put("double", 71.0);
        avro.put("boolean", true);
        avro.put("array", Arrays.asList(1, 2, 3));

        final org.apache.avro.Schema schema1 = schema.getField("object").schema();
        GenericRecord avroObject = new GenericData.Record(schema1);
        avroObject.put("f1", 12);
        avroObject.put("f2", "Hello");
        avro.put("object", avroObject);
    }

    @Test
    void inferSchema() {
        AvroToRecord toRecord = new AvroToRecord(recordBuilderFactory);
        Schema s = toRecord.inferSchema(avro);
        assertNotNull(s);
        assertEquals(7, s.getEntries().size());
        assertTrue(s.getType().equals(Type.RECORD));
        assertTrue(s.getEntries().stream().map(Entry::getName).collect(toList())
                .containsAll(Stream.of("string", "int", "long", "double", "boolean", "array", "object").collect(toList())));
    }

    @Test
    void toRecord() {
        AvroToRecord toRecord = new AvroToRecord(recordBuilderFactory);
        Record record = toRecord.toRecord(avro);
        assertNotNull(record);
        assertEquals("a string sample", record.getString("string"));
        assertEquals(710, record.getInt("int"));
        assertEquals(710L, record.getLong("long"));
        assertEquals(71.0, record.getDouble("double"));
        assertEquals(true, record.getBoolean("boolean"));

        final Collection<Integer> integers = record.getArray(Integer.class, "array");
        assertEquals(3, integers.size());
        assertTrue(integers.contains(1));
        assertTrue(integers.contains(2));
        assertTrue(integers.contains(3));

        final Record record1 = record.getRecord("object");
        assertEquals(12, record1.getInt("f1"));
        assertEquals("Hello", record1.getString("f2"));
    }

}