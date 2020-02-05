/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    private RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    protected Record versatileRecord;

    protected Record complexRecord;

    @Test
    void add() throws IOException {
        AvroConfiguration cfg = new AvroConfiguration();
        final RecordWriterSupplier writerSupplier = new AvroWriterSupplier();

        RecordWriter writer = writerSupplier.getWriter(() -> out, cfg);
        prepareTestRecords();
        writer.add(versatileRecord);
        writer.add(complexRecord);

        writer.flush();
        String res = out.toString();
        Assertions.assertFalse(res.isEmpty());

        writer.close();
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
}