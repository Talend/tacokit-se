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
package org.talend.components.common.stream.format.line.fixed;

import java.io.Serializable;
import java.util.Collections;
import java.util.Iterator;

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
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("lineConfiguration"), @GridLayout.Row("lengthFields") })
public class FixedConfiguration implements Serializable, RecordReaderSupplier {

    /** Serialization */
    private static final long serialVersionUID = -7116638194610358461L;

    /** constraint pattern for lengthFields (form "[number];[number]...> accepted */
    public static final String LengthPattern = "^([0-9]+;{0,1})*[0-9]+$";

    @Option
    @Documentation("line delimiter")
    private LineConfiguration lineConfiguration;

    @Option
    @Pattern(FixedConfiguration.LengthPattern)
    @Documentation("fields lengths separate by ';'")
    /** all fields length separate by ';' */
    private String lengthFields;

    /** fields length in table format */
    private int[] realLengthFields = new int[] {};

    /**
     * Redefine lengthFields setter to update realLengthFields.
     * 
     * @param lengthFields : new fields length string.
     */
    public void setLengthFields(String lengthFields) {
        this.lengthFields = lengthFields;
        if (this.isValid()) {
            realLengthFields = this.translateFields();
        } else {
            realLengthFields = new int[] {};
        }
    }

    /**
     * Test if lengthFields is valid.
     * 
     * @return true if ok.
     */
    public boolean isValid() {
        return lengthFields != null && lengthFields.matches(FixedConfiguration.LengthPattern);
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
        if (this.realLengthFields.length > 0) {
            return () -> new StringIterator(line, this.realLengthFields);
        }
        return Collections.emptyList();
    }

    /**
     * Get array of length from this.lengthFields ("23;12;3" -> [23, 12, 3])
     * 
     * @return array of length
     */
    private int[] translateFields() {
        String[] res = this.lengthFields.split(";");
        int[] lengths = new int[res.length];
        for (int i = 0; i < res.length; i++) {
            lengths[i] = Integer.valueOf(res[i]);
        }
        return lengths;
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
