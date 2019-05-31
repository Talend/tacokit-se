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

package org.talend.components.azure.runtime.converters;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.talend.components.azure.runtime.input.SchemaUtils;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class HTMLConverter implements RecordConverter<Element> {

    public static HTMLConverter of(RecordBuilderFactory recordBuilderFactory) {
        return new HTMLConverter(recordBuilderFactory);
    }

    private RecordBuilderFactory recordBuilderFactory;

    private Schema columns;

    private HTMLConverter(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @Override
    public Schema inferSchema(Element record) {
        if (columns == null) {
            List<String> columnNames = inferSchemaInfo(record, true);
            Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
            columnNames.forEach(column -> schemaBuilder
                    .withEntry(recordBuilderFactory.newEntryBuilder().withName(column).withType(Schema.Type.STRING).build()));
            columns = schemaBuilder.build();
        }
        return columns;
    }

    @Override
    public Record toRecord(Element record) {
        if (columns == null) {
            columns = inferSchema(record);
        }

        Record.Builder builder = recordBuilderFactory.newRecordBuilder();
        Elements rowColumns = record.getElementsByTag("td");
        for (int i = 0; i < rowColumns.size(); i++) {
            builder.withString(columns.getEntries().get(i).getName(), rowColumns.get(i).text());
        }
        return builder.build();
    }

    @Override
    public Element fromRecord(Record record) {
        throw new UnsupportedOperationException("HTML Output are not supporter");
    }

    private List<String> inferSchemaInfo(Element row, boolean useDefaultFieldName) {
        List<String> result = new ArrayList<>();
        Set<String> existNames = new HashSet<>();
        int index = 0;
        Elements columns = row.getElementsByTag("td");
        for (int i = 0; i < columns.size(); i++) {
            String fieldName = columns.get(i).ownText();
            if (useDefaultFieldName || StringUtils.isEmpty(fieldName)) {
                fieldName = "field" + i;
            }

            String finalName = SchemaUtils.correct(fieldName, index++, existNames);
            existNames.add(finalName);

            result.add(finalName);
        }
        return result;
    }
}
