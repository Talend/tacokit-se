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
package org.talend.components.common.stream.output.avro;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class AvroRecordWriterTest {

    private RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    protected Record versatileRecord;

    protected Record complexRecord;

    @Test
    void add() throws IOException {
        final AvroConfiguration cfg = new AvroConfiguration();
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RecordWriterSupplier writerSupplier = new AvroWriterSupplier();

        try (RecordWriter writer = writerSupplier.getWriter(() -> out, cfg)) {
            prepareTestRecords();
            writer.add(versatileRecord);
            writer.add(complexRecord);

            writer.flush();
            String res = out.toString();
            Assertions.assertFalse(res.isEmpty());
        }
    }

    @Test
    void addHeadLess() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroConfiguration cfg = new AvroConfiguration();
        cfg.setAttachSchema(false);
        final Schema avroSchema = SchemaBuilder.builder() //
                .record("testRecord") //
                .namespace("org.talend.test") //
                .fields() //
                .requiredInt("ID") //
                .requiredString("content") //
                .endRecord();
        cfg.setAvroSchema(avroSchema.toString(true));
        final RecordWriterSupplier writerSupplier = new AvroWriterSupplier();

        final Record record1 = factory.newRecordBuilder() //
                .withInt("ID", 125) //
                .withString("content", "Hello") //
                .build();
        final Record record2 = factory.newRecordBuilder() //
                .withInt("ID", 140) //
                .withString("content", "World") //
                .build();
        try (RecordWriter writer = writerSupplier.getWriter(() -> out, cfg)) {
            prepareTestRecords();
            writer.add(record1);
            writer.add(record2);

            writer.flush();
            String res = out.toString();
            Assertions.assertFalse(res.isEmpty());
        }
    }

    private void prepareTestRecords() {
        // some demo records
        versatileRecord = factory.newRecordBuilder() //
                .withString("string1", "Bonjour") //
                .withString("string2", "Olà") //
                .withInt("int", 71) //
                .withBoolean("boolean", true) //
                .withLong("long", 1971L) //
                .withDateTime("datetime", LocalDateTime.of(2019, 4, 22, 0, 0).atZone(ZoneOffset.UTC)) //
                .withFloat("float", 20.5f) //
                .withDouble("double", 20.5) //
                .build();

        Entry er = factory.newEntryBuilder().withName("record").withType(Type.RECORD)
                .withElementSchema(versatileRecord.getSchema()).build();
        Entry ea = factory.newEntryBuilder().withName("array").withType(Type.ARRAY)
                .withElementSchema(factory.newSchemaBuilder(Type.ARRAY).withType(Type.STRING).build()).build();

        complexRecord = factory.newRecordBuilder() //
                .withString("name", "ComplexR") //
                .withRecord(er, versatileRecord) //
                .withDateTime("now", ZonedDateTime.now()) //
                .withArray(ea, Arrays.asList("ary1", "ary2", "ary3")).build();
    }

    @Test
    void addComplex() throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final AvroConfiguration cfg = new AvroConfiguration();
        final RecordWriterSupplier writerSupplier = new AvroWriterSupplier();

        final RecordWriter writer = writerSupplier.getWriter(() -> out, cfg);
        final org.talend.sdk.component.api.record.Schema schemaSub = this.factory.newSchemaBuilder(Type.RECORD)
                .withEntry(factory.newEntryBuilder().withName("rec_string").withType(Type.STRING).withNullable(true).build())
                .withEntry(factory.newEntryBuilder().withName("rec_int").withType(Type.INT).withNullable(true).build()).build();
        final Record recordSub = this.factory.newRecordBuilder(schemaSub).withString("rec_string", "rec_string_1")
                .withInt("rec_int", 1).build();
        final Record recordSub1 = this.factory.newRecordBuilder(schemaSub).withString("rec_string", "rec_string_2")
                .withInt("rec_int", 2).build();

        final Entry entrySub = this.factory.newEntryBuilder().withName("sub").withNullable(true).withType(Type.RECORD)
                .withElementSchema(schemaSub).build();
        final org.talend.sdk.component.api.record.Schema schema = this.factory.newSchemaBuilder(Type.RECORD)
                .withEntry(this.factory.newEntryBuilder().withName("field1").withType(Type.STRING).withNullable(true).build())
                .withEntry(entrySub).build();

        final Record record = this.factory.newRecordBuilder(schema).withRecord(entrySub, recordSub).withString("field1", "value1")
                .build();
        final Record record1 = this.factory.newRecordBuilder(schema).withString("field1", "value2").build();
        final Record record2 = this.factory.newRecordBuilder(schema).withString("field1", "value3").withRecord(entrySub, null)
                .build();
        final Record record3 = this.factory.newRecordBuilder(schema).withRecord(entrySub, recordSub1)
                .withString("field1", "value2").build();

        writer.add(Arrays.asList(record2, record, record1));
        writer.add(record3);
        writer.add(record2);
        writer.add(record1);

        writer.flush();
        String res = out.toString();
        Assertions.assertFalse(res.isEmpty());

        writer.close();
    }

}