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

import java.io.InputStream;
import java.util.Iterator;

import org.talend.components.common.collections.IteratorMap;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.input.line.schema.HeaderHandler;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

/**
 * default class to read source composed of line (csv, fixed length ...) to records.
 */
public class DefaultRecordReader implements RecordReader {

    /** line reader */
    private final LineReader lineReader;

    /** translate line to record */
    private final LineTranslator<Record> toRecord;

    public static DefaultRecordReader of(RecordBuilderFactory factory, LineConfiguration lineConfig, LineSplitter splitter) {

        final LineToRecord toRecord = new LineToRecord(factory, splitter);
        final HeaderHandler headerHandler = new HeaderHandler(lineConfig.calcHeader(), toRecord::withHeaders);

        final LineReader lineReader = new DefaultLineReader(lineConfig.getLineSeparator(), lineConfig.getEncoding().getEncoding(),
                headerHandler);

        return new DefaultRecordReader(lineReader, toRecord);
    }

    public DefaultRecordReader(LineReader lineReader, LineTranslator<Record> toRecord) {
        this.lineReader = lineReader;
        this.toRecord = toRecord;
    }

    @Override
    public Iterator<Record> read(InputStream reader) {
        final Iterator<String> lines = lineReader.read(reader);
        return new IteratorMap<>(lines, this.toRecord::translate);
    }

    @Override
    public void close() {
        this.lineReader.close();
    }

}
