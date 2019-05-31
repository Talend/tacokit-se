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
package org.talend.components.adlsgen2.common.format.csv;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvIterator implements Iterator<Record> {

    private CSVFormat csv;

    private final Reader reader;

    private CsvConverter converter;

    private CSVParser parser;

    private Iterator<CSVRecord> records;

    private CsvIterator(Reader inReader, CSVFormat format) {
        csv = format;
        log.warn("[CsvIterator] csv{}", format);
        reader = inReader;
        converter = CsvConverter.of().withFormat(csv);
        log.warn("[CsvIterator] {}", converter);
        try {
            parser = csv.parse(reader);
            log.warn("[CsvIterator] parser: {} ", parser);
            records = parser.iterator();
            log.warn("[CsvIterator] records: {} ", records);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasNext() {
        return records.hasNext();
    }

    @Override
    public Record next() {

        if (hasNext()) {
            log.warn("[next] conv {}", converter);
            return converter.toRecord(records.next());
        } else {
            try {
                parser.close();
                reader.close();
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return null;
        }
    }

    public static class Builder {

        private CSVFormat csvFormat;

        private Builder() {
            csvFormat = CSVFormat.DEFAULT;
        }

        public static Builder of() {
            return new Builder();
        }

        public Builder withConfiguration(@Configuration("csvConfiguration") final CsvConfiguration configuration) {
            csvFormat = csvFormat //
                    .withDelimiter(configuration.getFieldDelimiter().getDelimiterChar()) //
                    .withRecordSeparator(configuration.getRecordDelimiter().getSeparatorChar()) //
                    .withHeader(configuration.getCsvSchema().split(configuration.getFieldDelimiter().getDelimiter())) //
            // TODO manage other parameters.
            ;
            if (configuration.isHeader()) {
                csvFormat = csvFormat.withFirstRecordAsHeader();
            }

            return this;
        }

        public CsvIterator parse(InputStream in) {
            // TODO manage fileEncoding
            return new CsvIterator(new InputStreamReader(in, StandardCharsets.UTF_8), csvFormat);
        }

        public CsvIterator parse(String content) {
            return new CsvIterator(new StringReader(content), csvFormat);
        }
    }
}
