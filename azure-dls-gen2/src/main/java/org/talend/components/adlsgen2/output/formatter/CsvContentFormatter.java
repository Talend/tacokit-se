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
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang.StringUtils;
import org.talend.components.adlsgen2.common.format.csv.CsvConfiguration;
import org.talend.components.adlsgen2.common.format.csv.CsvConverter;
import org.talend.components.adlsgen2.output.OutputConfiguration;
import org.talend.components.adlsgen2.runtime.AdlsGen2RuntimeException;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvContentFormatter implements ContentFormatter {

    private final OutputConfiguration configuration;

    private final CsvConfiguration csvConfiguration;

    private final CSVFormat format;

    private final CsvConverter converter;

    Schema schema;

    public CsvContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        csvConfiguration = this.configuration.getDataSet().getCsvConfiguration();
        converter = CsvConverter.of(recordBuilderFactory, csvConfiguration);
        format = converter.getCsvFormat();
    }

    @Override
    public byte[] prepareContent(List<Record> records) {
        if (records.isEmpty()) {
            return new byte[0];
        }
        // get schema from first record
        schema = records.get(0).getSchema();
        StringWriter stringWriter = new StringWriter();
        try {
            CSVPrinter printer = new CSVPrinter(stringWriter, format);
            if (csvConfiguration.isHeader()) {
                printer.print(getHeader());
            }
            for (Record record : records) {
                printer.printRecord(convertRecordToArray(record));
            }
            printer.flush();
            printer.close();
            return stringWriter.toString().getBytes();
        } catch (IOException e) {
            throw new AdlsGen2RuntimeException(e.getMessage());
        }
    }

    private String getHeader() {
        // first return user schema if exists
        if (StringUtils.isNotEmpty(csvConfiguration.getCsvSchema())) {
            log.warn("[getHeader] user schema");
            return csvConfiguration.getCsvSchema() + format.getRecordSeparator();
        }
        // otherwise record schema
        log.warn("[getHeader] record schema");
        StringBuilder headers = new StringBuilder(schema.getEntries().get(0).getName());
        for (int i = 1; i < schema.getEntries().size(); i++) {
            headers.append(format.getDelimiter()).append(schema.getEntries().get(i).getName());
        }
        return headers.append(format.getRecordSeparator()).toString();
    }

    private Object[] convertRecordToArray(Record record) {
        Object[] array = new Object[record.getSchema().getEntries().size()];
        for (int i = 0; i < schema.getEntries().size(); i++) {
            if (schema.getEntries().get(i).getType() == Schema.Type.DATETIME) {
                array[i] = record.getDateTime(schema.getEntries().get(i).getName());
            } else if (schema.getEntries().get(i).getType() == Schema.Type.BYTES) {
                array[i] = Arrays.toString(record.getBytes(schema.getEntries().get(i).getName()));
            } else {
                array[i] = record.get(Object.class, schema.getEntries().get(i).getName());
            }
        }
        return array;
    }

}
