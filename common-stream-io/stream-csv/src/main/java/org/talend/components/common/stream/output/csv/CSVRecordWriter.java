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
package org.talend.components.common.stream.output.csv;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Collections;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.talend.components.common.stream.CSVHelper;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.output.line.RecordSerializerLineHelper;
import org.talend.sdk.component.api.record.Record;

public class CSVRecordWriter implements RecordWriter {

    private final TargetFinder target;

    private final CSVConfiguration config;

    private CSVPrinter printer = null;

    public CSVRecordWriter(CSVConfiguration config, TargetFinder target) {
        this.target = target;
        this.config = config;
    }

    private void firstRecord(Record record) throws IOException {
        CSVFormat csvFormat = CSVHelper.getCsvFormat(config);

        int nbeHeaderLine = this.config.getLineConfiguration().calcHeader();
        if (nbeHeaderLine > 0) {
            csvFormat = csvFormat.withCommentMarker(' ');
            if (nbeHeaderLine > 2) {
                final String headers = String.join("", Collections.nCopies(nbeHeaderLine - 2, "\n"));
                csvFormat = csvFormat.withHeaderComments(headers);
            }
            final List<String> headers = RecordSerializerLineHelper.schemaFrom(record.getSchema());
            csvFormat = csvFormat.withHeader(headers.toArray(new String[] {}));
        }

        final OutputStream outputStream = this.target.find();
        final PrintStream ps = new PrintStream(outputStream);
        this.printer = csvFormat.print(ps);
    }

    @Override
    public void add(Record record) throws IOException {
        if (this.printer == null) {
            this.firstRecord(record);
        }
        final List<String> values = RecordSerializerLineHelper.valuesFrom(record);
        this.printer.printRecord(values);
    }

    @Override
    public void flush() throws IOException {
        this.printer.flush();
    }

    @Override
    public void close() throws IOException {
        this.printer.close(true);
    }
}
