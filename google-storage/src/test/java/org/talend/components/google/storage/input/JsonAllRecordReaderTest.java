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
package org.talend.components.google.storage.input;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Collection;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.input.json.JsonToRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonAllRecordReaderTest {

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @Test
    void read() throws IOException {
        final URL jsonResource = Thread.currentThread().getContextClassLoader().getResource("./data.json");
        try (final InputStream in = jsonResource.openStream();
                final JsonAllRecordReader reader = new JsonAllRecordReader(new JsonToRecord(factory, true))) {
            final Iterator<Record> recordIterator = reader.read(in);

            Assertions.assertTrue(recordIterator.hasNext());
            final Record record = recordIterator.next();
            Assertions.assertNotNull(record);

            Assertions.assertFalse(recordIterator.hasNext());
        }
    }

    @Test
    void readValue() {
        try (final JsonAllRecordReader reader = new JsonAllRecordReader(new JsonToRecord(factory, true))) {

            final Iterator<Record> records = reader.read(new ByteArrayInputStream("[1, 2]".getBytes()));
            Assertions.assertTrue(records.hasNext());
            final Record record = records.next();
            final Collection<Double> doubles = record.getArray(Double.class, "field");
            Assertions.assertEquals(2, doubles.size());
            Assertions.assertFalse(records.hasNext());
        }

    }
}