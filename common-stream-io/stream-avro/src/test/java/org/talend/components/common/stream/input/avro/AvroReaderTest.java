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

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.avro.AvroConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class AvroReaderTest {

    private static AvroToRecord toRecord;

    private static RecordBuilderFactory factory;

    @BeforeAll
    public static void init() {
        AvroReaderTest.factory = new RecordBuilderFactoryImpl("test");
        AvroReaderTest.toRecord = new AvroToRecord(AvroReaderTest.factory);
    }

    @Test
    public void testSupplier() {
        final AvroReaderSupplier supplier = new AvroReaderSupplier();
        final RecordReader reader = supplier.getReader(AvroReaderTest.factory, new AvroConfiguration());

        Assertions.assertNotNull(reader);
        reader.close();
    }

    @Test
    public void read() throws IOException {

        try (AvroReader reader = new AvroReader(toRecord);
                InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("./testAvroReader.avro")) {
            final Iterator<Record> iterator = reader.read(in);

            for (int i = 0; i < 10; i++) {
                Assertions.assertTrue(iterator.hasNext());
            }
            Record rec1 = this.assertNext(iterator);
            Record rec2 = this.assertNext(iterator);
            Record rec3 = this.assertNext(iterator);
            Record rec4 = this.assertNext(iterator);
            Record rec5 = this.assertNext(iterator);

            Assertions.assertFalse(iterator.hasNext());

            Assertions.assertEquals("test", rec2.getString("stringValue"));
            Assertions.assertEquals(true, rec4.getBoolean("booleanValue"));
        }
    }

    @Test
    public void readBadFormat() {
        boolean toException = false;
        try (AvroReader reader = new AvroReader(toRecord);
                InputStream in = Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("./testAvroReaderBadFormat.avro")) {
            final Iterator<Record> iterator = reader.read(in);

            Assertions.fail("should throw IOException");
        } catch (IllegalArgumentException | IOException | UncheckedIOException ex) {
            toException = true;
        }
        Assertions.assertTrue(toException, "no exception with bad format");
    }

    private Record assertNext(Iterator<Record> iterator) {
        Assertions.assertTrue(iterator.hasNext());
        Record rec = iterator.next();
        Assertions.assertNotNull(rec);
        return rec;
    }
}