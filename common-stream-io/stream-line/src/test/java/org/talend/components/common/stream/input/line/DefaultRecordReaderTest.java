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
package org.talend.components.common.stream.input.line;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.input.line.schema.HeaderHandler;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class DefaultRecordReaderTest {

    @Test
    void readLineFile() throws IOException {
        final URL sourceURL = Thread.currentThread().getContextClassLoader().getResource("./data.txt");

        final LineConfiguration config = new LineConfiguration();

        final LineSplitter splitter = (String line) -> Stream.of(line.substring(0, 3), //
                line.substring(3, 6), //
                line.substring(6)) //
                .collect(Collectors.toList());

        testContent(sourceURL.getPath(), splitter, config.getLineSeparator());
    }

    /**
     * Test function.
     * 
     * @param filePath : path for source file.
     * @param splitter : split line in fields.
     * @param lineSeparator : line separator.
     * @throws IOException : if fails.
     */
    private void testContent(String filePath, LineSplitter splitter, String lineSeparator) throws IOException {
        HeaderHandler handler = new HeaderHandler(0, null);
        LineReader lineReader = new DefaultLineReader(lineSeparator, "UTF-8", handler);

        LineToRecord toRecord = new LineToRecord(new RecordBuilderFactoryImpl("test"), splitter);

        try (DefaultRecordReader recordReader = new DefaultRecordReader(lineReader, toRecord);
                InputStream reader = new FileInputStream(filePath)) {
            final Iterator<Record> recordIterator = recordReader.read(reader);
            for (int i = 0; i < 5; i++) {
                Assertions.assertTrue(recordIterator.hasNext()); // hasNext must be re-entrant.
            }
            Record rec = recordIterator.next();
            Assertions.assertNotNull(rec);
            Assertions.assertEquals("One", rec.getString("field_1"));
            Assertions.assertEquals("Two", rec.getString("field_2"));
            Assertions.assertEquals("33", rec.getString("field_3"));

            Assertions.assertTrue(recordIterator.hasNext());
            rec = recordIterator.next();
            Assertions.assertNotNull(rec);
            Assertions.assertEquals("A1a", rec.getString("field_1"));
            Assertions.assertEquals("B1b", rec.getString("field_2"));
            Assertions.assertEquals("01", rec.getString("field_3"));
            Assertions.assertTrue(recordIterator.hasNext());
            rec = recordIterator.next();
            Assertions.assertNotNull(rec);
            Assertions.assertEquals("A2a", rec.getString("field_1"));
            Assertions.assertEquals("B2b", rec.getString("field_2"));
            Assertions.assertEquals("01", rec.getString("field_3"));

            Assertions.assertFalse(recordIterator.hasNext());
        }

    }
}