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
package org.talend.components.common.stream.input.json;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.record.Record;
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
}