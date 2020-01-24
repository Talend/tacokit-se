/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.input.fixed;

import java.util.Collections;
import java.util.Iterator;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderRepository;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.FixedConfiguration;
import org.talend.components.common.stream.format.JsonConfiguration;
import org.talend.components.common.stream.input.line.DefaultLineReader;
import org.talend.components.common.stream.input.line.DefaultRecordReader;
import org.talend.components.common.stream.input.line.LineReader;
import org.talend.components.common.stream.input.line.LineSplitter;
import org.talend.components.common.stream.input.line.LineToRecord;
import org.talend.components.common.stream.input.line.LineTranslator;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class FixedReaderSupplier implements RecordReaderSupplier {

    static {
        RecordReaderRepository.getInstance().put(FixedConfiguration.class, new FixedReaderSupplier());
    }

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config) {
        assert config instanceof FixedConfiguration : "fixed reader not with fixed config";
        FixedConfiguration fixedConfig = (FixedConfiguration) config;
        String lineSep = fixedConfig.getLineConfiguration().getLineSeparator();
        final LineReader lineReader = new DefaultLineReader(lineSep);
        final LineSplitter splitter = new FixedLineSplitter(fixedConfig.getRealLengthFields());
        final LineTranslator<Record> toRecord = new LineToRecord(factory, splitter);

        return new DefaultRecordReader(lineReader, toRecord);
    }

    static class FixedLineSplitter implements LineSplitter {

        private final int[] fieldLengths;

        public FixedLineSplitter(int[] fieldLengths) {
            this.fieldLengths = fieldLengths;
        }

        @Override
        public Iterable<String> translate(String line) {
            if (this.fieldLengths != null && this.fieldLengths.length > 0) {
                return () -> new FixedReaderSupplier.StringIterator(line, this.fieldLengths);
            }
            return Collections.emptyList();
        }
    }

    /**
     * Iterator class for field in line of fixed data.
     * "FooHello" => "Foo", "Hello" with length [3, 5]
     */
    static class StringIterator implements Iterator<String> {

        /** line of data */
        private final String line;

        /** cursor index in line */
        private int lineCursor = 0;

        /** fields lengths */
        private final int[] fieldLengths;

        /** current index for fieldLengths */
        private int fieldLengthsIndex = 0;

        /** value for current field */
        private String currentFieldValue = null;

        public StringIterator(String line, int[] translatedLengthField) {
            this.line = line;
            this.fieldLengths = translatedLengthField;
            this.next();
        }

        @Override
        public boolean hasNext() {
            return currentFieldValue != null;
        }

        @Override
        public final String next() {
            final String result = currentFieldValue;
            if (this.fieldLengthsIndex < this.fieldLengths.length) {
                int posEnd = this.lineCursor + this.fieldLengths[this.fieldLengthsIndex];
                this.currentFieldValue = this.line.substring(lineCursor, posEnd);
                this.lineCursor = posEnd;
                this.fieldLengthsIndex++;
            } else {
                this.currentFieldValue = null;
            }
            return result;
        }
    }

}
