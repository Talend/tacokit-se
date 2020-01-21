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
package org.talend.components.common.stream.input.line;

import java.io.Reader;
import java.util.Iterator;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.sdk.component.api.record.Record;

public class DefaultRecordReader implements RecordReader {

    private final LineReader lineReader;

    private final LineTranslator<Record> toRecord;

    public DefaultRecordReader(LineReader lineReader, LineTranslator<Record> toRecord) {
        this.lineReader = lineReader;
        this.toRecord = toRecord;
    }

    @Override
    public Iterator<Record> read(Reader reader) {
        final Iterator<String> lines = lineReader.read(reader);
        return new RecordIterator(this.toRecord, lines);
    }

    @Override
    public void close() {
        this.lineReader.close();
    }

    static class RecordIterator implements Iterator<Record> {

        private final LineTranslator<Record> toRecord;

        private final Iterator<String> lines;

        public RecordIterator(LineTranslator<Record> toRecord, Iterator<String> lines) {
            this.toRecord = toRecord;
            this.lines = lines;
        }

        @Override
        public boolean hasNext() {
            return this.lines.hasNext();
        }

        @Override
        public Record next() {
            String line = this.lines.next();
            if (line != null) {
                return this.toRecord.translate(line);
            }
            return null;
        }
    }
}
