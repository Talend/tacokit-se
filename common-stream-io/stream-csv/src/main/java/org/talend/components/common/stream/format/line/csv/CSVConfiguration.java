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
package org.talend.components.common.stream.format.line.csv;

import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.line.LineConfiguration;
import org.talend.components.common.stream.input.line.DefaultLineReader;
import org.talend.components.common.stream.input.line.DefaultRecordReader;
import org.talend.components.common.stream.input.line.LineReader;
import org.talend.components.common.stream.input.line.LineSplitter;
import org.talend.components.common.stream.input.line.LineToRecord;
import org.talend.components.common.stream.input.line.LineTranslator;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("lineConfiguration"), @GridLayout.Row({ "fieldSeparator" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "escape", "quotedValue" }) })
public class CSVConfiguration implements Serializable, RecordReaderSupplier {

    private static final long serialVersionUID = -6803208558417743486L;

    @Option
    @Documentation("line delimiter")
    private LineConfiguration lineConfiguration;

    @Option
    @Documentation("field delimiter")
    private Character fieldSeparator;

    @Option
    @Documentation("Escape character")
    private Character escape = '\\';

    @Option
    @Documentation("Text enclosure character")
    private Character quotedValue = '"';

    private transient CSVFormat csvFormat = null;

    public void setFieldSeparator(Character fieldSeparator) {
        this.fieldSeparator = fieldSeparator;
        this.csvFormat = null; // field sep change => csvFormat must change
    }

    public void setEscape(Character escape) {
        this.escape = escape;
        this.csvFormat = null; // escape char change => csvFormat must change
    }

    public void setQuotedValue(Character quotedValue) {
        this.quotedValue = quotedValue;
        this.csvFormat = null; // quoted char change => csvFormat must change
    }

    private CSVFormat getCsvFormat() {
        if (this.csvFormat == null) {
            this.csvFormat = CSVFormat.newFormat(this.fieldSeparator).withQuote(this.quotedValue).withEscape(this.escape);
        }
        return this.csvFormat;
    }

    @Override
    public RecordReader getReader(RecordBuilderFactory factory) {
        final LineReader lineReader = new DefaultLineReader(this.lineConfiguration.getLineSeparator());
        final LineSplitter splitter = this::treat;
        final LineTranslator<Record> toRecord = new LineToRecord(factory, splitter);

        return new DefaultRecordReader(lineReader, toRecord);
    }

    /**
     * extract fields values from a fixed line.
     *
     * @param line : line of data.
     * @return all value fields.
     */
    public Iterable<String> treat(String line) {
        try {
            final CSVParser parser = CSVParser.parse(line, this.getCsvFormat());
            final List<CSVRecord> records = parser.getRecords();
            if (records.isEmpty()) {
                return Collections.emptyList();
            }
            return records.get(0);
        } catch (IOException e) {
            throw new UncheckedIOException("Unparsable CSV line '" + line + "'", e);
        }
    }
}
