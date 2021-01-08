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
package org.talend.components.common.stream.input.csv;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.List;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.talend.components.common.stream.CSVHelper;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.input.line.DefaultRecordReader;
import org.talend.components.common.stream.input.line.LineSplitter;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class CSVReaderSupplier implements RecordReaderSupplier {

    @Override
    public RecordReader getReader(RecordBuilderFactory factory, ContentFormat config, Object extraParameter) {
        if (!CSVConfiguration.class.isInstance(config)) {
            throw new IllegalArgumentException("try to get csv-reader with other than csv config");
        }

        final CSVConfiguration csvConfig = (CSVConfiguration) config;
        final CSVFormat csvFormat = CSVHelper.getCsvFormat(csvConfig);
        final LineSplitter splitter = new CSVLineSplitter(csvFormat);

        return DefaultRecordReader.of(factory, csvConfig.getLineConfiguration(), splitter);
    }

    static class CSVLineSplitter implements LineSplitter {

        private final CSVFormat format;

        public CSVLineSplitter(CSVFormat format) {
            this.format = format;
        }

        /**
         * extract fields values from a fixed line.
         *
         * @param line : line of data.
         * @return all value fields.
         */
        @Override
        public Iterable<String> translate(String line) {
            try {
                final CSVParser parser = CSVParser.parse(line, this.format);
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
}
