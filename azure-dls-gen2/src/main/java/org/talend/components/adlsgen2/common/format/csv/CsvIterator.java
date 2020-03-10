/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
import java.io.Serializable;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;

import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvIterator implements Iterator<Record>, Serializable {

    private final Reader reader;

    private CsvConverter converter;

    private CSVParser parser;

    private Iterator<CSVRecord> records;

    private CsvIterator(CsvConverter converter, Reader inReader) {
        reader = inReader;
        this.converter = converter;
        try {
            parser = this.converter.getCsvFormat().parse(reader);
            // in the case of no schema is defined in the config and there's a header set in config
            // to keep ordering we're obliged to do this
            // will try to find a better workaround later...
            converter.setRuntimeHeaders(parser.getHeaderMap());
            records = parser.iterator();
        } catch (IOException e) {
            throw new FileFormatRuntimeException(e.getMessage());
        }
    }

    @Override
    public boolean hasNext() {
        return records.hasNext();
    }

    @Override
    public Record next() {
        if (hasNext()) {
            return converter.toRecord(records.next());
        } else {
            try {
                parser.close();
                reader.close();
            } catch (IOException e) {
                throw new FileFormatRuntimeException(e.getMessage());
            }
            return null;
        }
    }

    public static class Builder {

        private CsvConverter converter;

        private CsvConfiguration configuration;

        private RecordBuilderFactory factory;

        private Builder(final RecordBuilderFactory factory) {
            this.factory = factory;
        }

        public static Builder of(final RecordBuilderFactory factory) {
            return new Builder(factory);
        }

        public Builder withConfiguration(@Configuration("csvConfiguration") final CsvConfiguration configuration) {
            log.debug("[Builder::withConfiguration] conf: {}", configuration);
            this.configuration = configuration;
            converter = CsvConverter.of(factory, configuration);

            return this;
        }

        public CsvIterator parse(InputStream in) {
            try {
                return new CsvIterator(converter, new InputStreamReader(in, configuration.effectiveFileEncoding()));
            } catch (UnsupportedEncodingException e) {
                log.error("[parse] {}", e.getMessage());
                throw new FileFormatRuntimeException(e.getMessage());
            }
        }

        public CsvIterator parse(String content) {
            return new CsvIterator(converter, new StringReader(content));
        }
    }
}
