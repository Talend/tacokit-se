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
 *
 */
package org.talend.components.adlsgen2.output.formatter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.talend.components.adlsgen2.common.format.csv.CsvConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvContentFormatter implements ContentFormatter {

    private final OutputConfiguration configuration;

    private RecordBuilderFactory recordBuilderFactory;

    CsvConverter converter;

    public CsvContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.configuration = configuration;
        converter = CsvConverter.of(recordBuilderFactory, configuration.getDataSet().getCsvConfiguration());
    }

    @Override
    public byte[] prepareContent(List<Record> records) {
        StringBuilder sb = new StringBuilder();
        for (Record record : records) {
            sb.append(toCsvFormat(record));
        }
        return sb.toString().getBytes();
    }

    private String[] getStringArrayFromRecord(Record record) {
        List<String> values = new ArrayList<>();
        for (Schema.Entry field : record.getSchema().getEntries()) {
            values.add(record.getString(field.getName()));
        }
        return values.toArray(new String[0]);
    }

    private String toCsvFormat(final Record record) {
        StringWriter writer = new StringWriter();
        CSVFormat csv = converter.getCsvFormat();
        try {
            CSVPrinter printer = new CSVPrinter(writer, csv);
            printer.printRecord(getStringArrayFromRecord(record));
            return writer.toString();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

}
