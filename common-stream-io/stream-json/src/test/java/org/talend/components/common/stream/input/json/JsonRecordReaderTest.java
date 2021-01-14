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
package org.talend.components.common.stream.input.json;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonRecordReaderTest {

    @Test
    void read() {
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonConfiguration jsonCfg = new JsonConfiguration();
        jsonCfg.setJsonPointer("/arrayfield");

        JsonReaderSupplier supplier = new JsonReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, jsonCfg);

        final InputStream input = inputTest();
        final Iterator<Record> recordIterator = reader.read(input);
        for (int i = 0; i < 5; i++) {
            Assertions.assertTrue(recordIterator.hasNext()); // re-entrant.
        }
        testRecord(recordIterator, 1);
        testRecord(recordIterator, 2);
        testRecord(recordIterator, 3);
        Assertions.assertFalse(recordIterator.hasNext());

        reader.close();
    }

    private void testRecord(Iterator<Record> recordIterator, int value) {
        Assertions.assertTrue(recordIterator.hasNext());
        final Record rec = recordIterator.next();
        Assertions.assertNotNull(rec);
        Assertions.assertEquals(value, rec.getInt("value"));
    }

    private InputStream inputTest() {
        StringBuilder builder = new StringBuilder();
        builder.append("{" + System.lineSeparator());
        builder.append("\t\"arrayfield\": [" + System.lineSeparator());

        builder.append("\t\t{\"value\": 1}," + System.lineSeparator());
        builder.append("\t\t{\"value\": 2}," + System.lineSeparator());
        builder.append("\t\t{\"value\": 3}" + System.lineSeparator());

        builder.append("\t]" + System.lineSeparator());
        return new ByteArrayInputStream(builder.toString().getBytes());
    }

    @Test
    void toRecordWithArray() {
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonConfiguration jsonCfg = new JsonConfiguration();
        jsonCfg.setJsonPointer("/");

        JsonReaderSupplier supplier = new JsonReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, jsonCfg);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("corona-api.countries.json");
        final Iterator<Record> recordIterator = reader.read(inputStream);

        Assertions.assertTrue(recordIterator.hasNext());
        final Record next = recordIterator.next();

        Assertions.assertEquals(Schema.Type.RECORD, next.getSchema().getType());
        Assertions.assertEquals(Schema.Type.ARRAY, next.getSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.RECORD, next.getSchema().getEntries().get(0).getElementSchema().getType());

        Assertions.assertFalse(recordIterator.hasNext());
    }

    @Test
    void toRecordWithRootArray() {
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonConfiguration jsonCfg = new JsonConfiguration();
        jsonCfg.setJsonPointer("/");

        JsonReaderSupplier supplier = new JsonReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, jsonCfg);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("corona-api.countries2.json");
        final Iterator<Record> recordIterator = reader.read(inputStream);

        Assertions.assertTrue(recordIterator.hasNext());

        final Record next = recordIterator.next();
        Assertions.assertEquals(Schema.Type.RECORD, next.getSchema().getType());

        List<Object[]> expectedSchema = new ArrayList<>();
        expectedSchema.add(new Object[] { "coordinates", Schema.Type.RECORD });
        expectedSchema.add(new Object[] { "name", Schema.Type.STRING });
        expectedSchema.add(new Object[] { "code", Schema.Type.STRING });
        expectedSchema.add(new Object[] { "population", Schema.Type.DOUBLE });
        expectedSchema.add(new Object[] { "updated_at", Schema.Type.STRING });
        expectedSchema.add(new Object[] { "today", Schema.Type.RECORD });
        expectedSchema.add(new Object[] { "latest_data", Schema.Type.RECORD });

        int i = 0;
        for (Schema.Entry entry : next.getSchema().getEntries()) {
            Assertions.assertEquals((String) expectedSchema.get(i)[0], entry.getName());
            Assertions.assertEquals((Schema.Type) expectedSchema.get(i)[1], entry.getType(),
                    "Wrong schema for " + expectedSchema.get(i)[0]);
            i++;
        }

        // Two records
        Assertions.assertTrue(recordIterator.hasNext());
        recordIterator.next();
        Assertions.assertFalse(recordIterator.hasNext());
    }

    @Test
    void toRecordWithNestedArrays() {
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonConfiguration jsonCfg = new JsonConfiguration();
        jsonCfg.setJsonPointer("/");
        jsonCfg.setForceDouble(false);

        JsonReaderSupplier supplier = new JsonReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, jsonCfg);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("corona-api.countries3.json");
        final Iterator<Record> recordIterator = reader.read(inputStream);

        Assertions.assertTrue(recordIterator.hasNext());
        final Record next = recordIterator.next();
        final Entry firstField = next.getSchema().getEntries().get(0);
        Assertions.assertEquals(Schema.Type.ARRAY, firstField.getType());
        Assertions.assertEquals(Schema.Type.RECORD, firstField.getElementSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.ARRAY,
                firstField.getElementSchema().getEntries().get(0).getElementSchema().getEntries().get(0).getType());
        Assertions.assertEquals(Schema.Type.RECORD, firstField.getElementSchema().getEntries().get(0).getElementSchema()
                .getEntries().get(0).getElementSchema().getType());

        Assertions.assertEquals(Schema.Type.ARRAY,
                firstField.getElementSchema().getEntries().get(0).getElementSchema().getEntries().get(1).getType());
        Assertions.assertEquals(Schema.Type.LONG, firstField.getElementSchema().getEntries().get(0).getElementSchema()
                .getEntries().get(1).getElementSchema().getType());

        final Record coordinates = next.getArray(Record.class, "data").iterator().next().getRecord("coordinates");
        Assertions.assertEquals(1.0d, coordinates.getArray(Record.class, "latitude").iterator().next().getDouble("aa"));
        Assertions.assertEquals(1, coordinates.getArray(Long.class, "longitude").iterator().next());

        Assertions.assertFalse(recordIterator.hasNext());

    }

    @Test
    void toRecordWithArrayOfArrays() {
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        JsonConfiguration jsonCfg = new JsonConfiguration();
        jsonCfg.setJsonPointer("/");

        JsonReaderSupplier supplier = new JsonReaderSupplier();
        final RecordReader reader = supplier.getReader(factory, jsonCfg);

        InputStream inputStream = getClass().getClassLoader().getResourceAsStream("arrayOfArrays.json");
        final Iterator<Record> recordIterator = reader.read(inputStream);

        Assertions.assertTrue(recordIterator.hasNext());
        final Record next = recordIterator.next();

        Assertions.assertFalse(recordIterator.hasNext());

    }

}