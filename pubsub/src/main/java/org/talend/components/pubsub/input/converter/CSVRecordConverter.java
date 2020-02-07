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
package org.talend.components.pubsub.input.converter;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVRecordConverter {

    @Getter
    private CSVFormat csvFormat;

    @Getter
    private Schema schema;

    public RecordBuilderFactory recordBuilderFactory;

    private CSVRecordConverter(RecordBuilderFactory recordBuilderFactory, char delimiter) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.csvFormat = createCSVFormat(delimiter, "\n", "\"", "\\");
    }

    private CSVFormat createCSVFormat(char fieldDelimiter, String recordDelimiter, String textEnclosure, String escapeChar) {
        // CSVFormat.RFC4180 use " as quote and no escape char and "," as field
        // delimiter and only quote if quote is set and necessary
        CSVFormat format = CSVFormat.RFC4180.withDelimiter(fieldDelimiter);

        Character textEnclosureCharacter = null;
        if (StringUtils.isNotEmpty(textEnclosure)) {
            textEnclosureCharacter = textEnclosure.charAt(0);
        }

        Character enclosureChar = null;
        if (StringUtils.isNotEmpty(escapeChar)) {
            enclosureChar = escapeChar.charAt(0);
        }

        // the with method return a new object, so have to assign back
        if (textEnclosureCharacter != null) {
            format = format.withQuote(textEnclosureCharacter);
        } else {
            format = format.withQuote(null);
        }

        if (enclosureChar != null) {
            format = format.withEscape(enclosureChar);
        }

        if (StringUtils.isNoneEmpty(recordDelimiter)) {
            format = format.withRecordSeparator(recordDelimiter);
        }

        return format;
    }

    public static CSVRecordConverter of(RecordBuilderFactory recordBuilderFactory, char delimiter) {
        return new CSVRecordConverter(recordBuilderFactory, delimiter);
    }

    public Schema inferSchema(CSVRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        for (int i = 0; i < record.size(); i++) {
            Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
            String fieldName = "field" + i;
            builder.withEntry(entryBuilder.withName(fieldName).withType(Schema.Type.STRING).build());
        }
        return builder.build();
    }

    public Record toRecord(CSVRecord value) {
        if (schema == null) {
            schema = inferSchema(value);
        }

        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        for (int i = 0; i < schema.getEntries().size(); i++) {
            recordBuilder.withString(schema.getEntries().get(i), value.get(i));
        }
        return recordBuilder.build();
    }
}