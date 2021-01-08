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
package org.talend.components.common.stream.format.line.fixed;

import java.io.ByteArrayInputStream;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.Encoding;
import org.talend.components.common.stream.format.Encoding.Type;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.format.fixed.FixedConfiguration;
import org.talend.components.common.stream.input.fixed.FixedReaderSupplier;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class FixedLineRecordTest {

    @Test
    void getReader() {
        FixedConfiguration cfg = new FixedConfiguration();
        cfg.setLengthFields("3;4;5");
        cfg.setLineConfiguration(new LineConfiguration());
        cfg.getLineConfiguration().setLineSeparator(System.lineSeparator());
        cfg.getLineConfiguration().setEncoding(new Encoding());
        cfg.getLineConfiguration().getEncoding().setEncodingType(Type.UTF8);

        FixedReaderSupplier recordReaderSupplier = new FixedReaderSupplier();
        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final RecordReader reader = recordReaderSupplier.getReader(factory, cfg);

        String testInput = "Hi!GuysHello" + System.lineSeparator() + "abc1234ABCDE";

        final Iterator<Record> records = reader.read(new ByteArrayInputStream(testInput.getBytes()));

        for (int i = 0; i < 5; i++) {
            Assertions.assertTrue(records.hasNext()); // reentrant test.
        }
        Record rec1 = records.next();
        Assertions.assertEquals("Hi!", rec1.getString("field_1"));
        Assertions.assertEquals("Guys", rec1.getString("field_2"));
        Assertions.assertEquals("Hello", rec1.getString("field_3"));

        Assertions.assertTrue(records.hasNext());
        Record rec2 = records.next();
        Assertions.assertEquals("abc", rec2.getString("field_1"));
        Assertions.assertEquals("1234", rec2.getString("field_2"));
        Assertions.assertEquals("ABCDE", rec2.getString("field_3"));

        Assertions.assertFalse(records.hasNext());
    }

}