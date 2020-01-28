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
package org.talend.components.common.stream.output.csv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.components.common.stream.output.line.RecordSerializerLineHelper;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class CSVRecordConverter implements RecordConverter<byte[], byte[]> {

    private final CSVFormat format;

    public CSVRecordConverter(CSVFormat format) {
        this.format = format;
    }

    @Override
    public byte[] fromRecord(Record record) {
        final List<String> values = RecordSerializerLineHelper.valuesFrom(record);
        return this.concat(values);
    }

    @Override
    public byte[] fromRecordSchema(Schema schema) {
        final List<String> values = RecordSerializerLineHelper.schemaFrom(schema);
        return this.concat(values);
    }

    private byte[] concat(List<String> values) {
        final StringBuilder builder = new StringBuilder();

        try {
            this.format.printRecord(builder, values.toArray());
            return builder.toString().getBytes(Charset.defaultCharset());
        } catch (IOException exIO) {
            throw new UncheckedIOException("Unable to transform record in CSV " + exIO.getMessage(), exIO);
        }
    }
}
