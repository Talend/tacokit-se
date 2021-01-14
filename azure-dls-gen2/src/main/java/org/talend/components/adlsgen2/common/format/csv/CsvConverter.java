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
package org.talend.components.adlsgen2.common.format.csv;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CsvConverter implements RecordConverter<CSVRecord>, Serializable {

    @Getter
    private CSVFormat csvFormat;

    private RecordBuilderFactory recordBuilderFactory;

    @Getter
    private Schema schema;

    @Setter
    private Map<String, Integer> runtimeHeaders;

    private CsvConverter(final RecordBuilderFactory factory,
            final @Configuration("csvConfiguration") CsvConfiguration configuration) {
        recordBuilderFactory = factory;
        csvFormat = formatWithConfiguration(configuration);
        schema = schemaWithConfiguration(configuration);
        log.debug("[CsvConverter] format: {}, schema: {}", csvFormat, schema);
    }

    public static CsvConverter of(final RecordBuilderFactory factory,
            final @Configuration("csvConfiguration") CsvConfiguration configuration) {
        return new CsvConverter(factory, configuration);
    }

    private Schema schemaWithConfiguration(CsvConfiguration configuration) {
        if (StringUtils.isEmpty(configuration.getCsvSchema())) {
            // will infer schema on runtime
            return null;
        }
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();
        int index = 0;
        for (String s : configuration.getCsvSchema().split(String.valueOf(configuration.effectiveFieldDelimiter()))) {
            Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            String finalName = RecordConverter.getCorrectSchemaFieldName(s, index++, existNames);
            existNames.add(finalName);
            builder.withEntry(entryBuilder.withName(finalName).withType(Schema.Type.STRING).withNullable(true).build());
        }

        return builder.build();
    }

    private CSVFormat formatWithConfiguration(@Configuration("csvConfiguration") final CsvConfiguration configuration) {
        log.debug("[CsvConverter::formatWithConfiguration] {}", configuration);
        char delimiter = configuration.effectiveFieldDelimiter();
        String separator = configuration.effectiveRecordSeparator();
        String escape = configuration.getEscapeCharacter();
        String enclosure = configuration.getTextEnclosureCharacter();
        String confSchema = configuration.getCsvSchema();
        CSVFormat format = CSVFormat.DEFAULT;
        // delimiter
        format = format.withDelimiter(delimiter);
        // record separator
        if (StringUtils.isNotEmpty(separator)) {
            format = format.withRecordSeparator(separator);
        }
        // escape character
        if (StringUtils.isNotEmpty(escape) && escape.length() == 1) {
            format = format.withEscape(escape.charAt(0));
        }
        // text enclosure
        if (StringUtils.isNotEmpty(enclosure) && enclosure.length() == 1) {
            format = format.withQuote(enclosure.charAt(0));
            format = format.withQuoteMode(QuoteMode.ALL);
        } else {
            // CSVFormat.DEFAULT has quotes defined
            format = format.withQuote(null);
        }
        // first line is header
        if (configuration.isHeader()) {
            format = format.withFirstRecordAsHeader();
        }
        // header columns
        if (configuration.isHeader() && StringUtils.isNotEmpty(confSchema)) {
            format = format.withHeader(confSchema.split(String.valueOf(delimiter)));
        }

        return format;
    }

    @Override
    public Schema inferSchema(CSVRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();
        String finalName;
        int index = 0;
        // record.toMap() return an unsorted map, so will loose fields ordering.
        // see CsvIterator constructor.
        if (runtimeHeaders != null) {
            for (Entry<String, Integer> f : runtimeHeaders.entrySet()) {
                Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
                finalName = RecordConverter.getCorrectSchemaFieldName(f.getKey(), index++, existNames);
                existNames.add(finalName);
                builder.withEntry(entryBuilder.withName(finalName).withType(Schema.Type.STRING).withNullable(true).build());
            }
        } else {
            for (int i = 0; i < record.size(); i++) {
                Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
                finalName = "field" + i;
                builder.withEntry(entryBuilder.withName(finalName).withType(Schema.Type.STRING).withNullable(true).build());
            }
        }
        return builder.build();
    }

    @Override
    public Record toRecord(CSVRecord csvRecord) {
        if (schema == null) {
            schema = inferSchema(csvRecord);
        }
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        for (int i = 0; i < schema.getEntries().size(); i++) {
            String value;
            try {
                value = csvRecord.get(i).isEmpty() ? null : csvRecord.get(i);
            } catch (ArrayIndexOutOfBoundsException e) {
                value = null;
            }
            recordBuilder.withString(schema.getEntries().get(i), value);
        }

        return recordBuilder.build();
    }

    @Override
    public CSVRecord fromRecord(Record record) {
        throw new UnsupportedOperationException("#fromRecord()");
    }

}
