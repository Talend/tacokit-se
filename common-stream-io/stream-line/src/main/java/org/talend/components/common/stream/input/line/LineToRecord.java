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
package org.talend.components.common.stream.input.line;

import java.util.List;

import org.talend.components.common.stream.input.line.schema.SchemaBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

/**
 * Translate line from source to record.
 */
public class LineToRecord implements LineTranslator<Record> {

    /** record factory */
    private final RecordBuilderFactory recordBuilderFactory;

    /** split line in serie of values. */
    private final LineSplitter splitter;

    private final SchemaBuilder schemaBuilder = new SchemaBuilder();

    public LineToRecord(RecordBuilderFactory recordBuilderFactory, LineSplitter splitter) {
        this.recordBuilderFactory = recordBuilderFactory;
        this.splitter = splitter;
    }

    /**
     * Build schema with header line.
     * 
     * @param headersLine header from header line.
     */
    public void withHeaders(String headersLine) {
        final Iterable<String> headers = splitter.translate(headersLine);
        this.schemaBuilder.get(this.recordBuilderFactory, headers, true);
    }

    @Override
    public Record translate(String line) {
        final Iterable<String> fields = splitter.translate(line);
        return this.build(fields);
    }

    private Record build(Iterable<String> fields) {
        final Schema schema = this.schemaBuilder.get(this.recordBuilderFactory, fields, false);
        final Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);

        final List<Entry> entries = schema.getEntries();
        int indexEntry = 0;
        for (String field : fields) {
            recordBuilder.withString(entries.get(indexEntry), field);
            indexEntry++;
        }
        return recordBuilder.build();
    }

}
