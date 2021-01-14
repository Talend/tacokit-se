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
package org.talend.components.adlsgen2.runtime.formatter;

import java.io.IOException;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.lang3.StringUtils;
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
public class CsvContentFormatter extends AbstractContentFormatter {

    private final OutputConfiguration configuration;

    private final CsvConfiguration csvConfiguration;

    private final CSVFormat format;

    private final CsvConverter converter;

    private Schema schema;

    public CsvContentFormatter(@Option("configuration") final OutputConfiguration configuration,
            final RecordBuilderFactory recordBuilderFactory) {
        this.configuration = configuration;
        csvConfiguration = this.configuration.getDataSet().getCsvConfiguration();
        converter = CsvConverter.of(recordBuilderFactory, csvConfiguration);
        format = converter.getCsvFormat();
    }

    @Override
    public byte[] feedContent(List<Record> records) {
        if (records.isEmpty()) {
            return new byte[0];
        }
        // get schema from first record
        schema = records.get(0).getSchema();
        StringWriter stringWriter = new StringWriter();
        try {
            CSVPrinter printer = new CSVPrinter(stringWriter, format);
            if (csvConfiguration.isHeader()) {
                printer.printRecord(getHeader());
            }
            for (Record record : records) {
                printer.printRecord(convertRecordToArray(record));
            }
            printer.flush();
            printer.close();
            return stringWriter.toString().getBytes(csvConfiguration.effectiveFileEncoding());
        } catch (IOException e) {
            log.error("[feedContent] {}", e.getMessage());
            throw new AdlsGen2RuntimeException(e.getMessage());
        }
    }

    private Object[] getHeader() {
        // cannot be called in initializeContent because we may need a least one record...
        // first return user schema if exists
        if (StringUtils.isNotEmpty(csvConfiguration.getCsvSchema())) {
            log.info("[getHeader] user schema");
            List<String> result = csvConfiguration.getCsvSchemaHeaders();
            if (result != null && !result.isEmpty()) {
                return result.toArray();
            }
        }
        // otherwise record schema
        List<String> headers = new ArrayList<>();
        log.info("[getHeader] record schema");
        for (int i = 0; i < schema.getEntries().size(); i++) {
            headers.add(schema.getEntries().get(i).getName());
        }
        return headers.toArray();
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
