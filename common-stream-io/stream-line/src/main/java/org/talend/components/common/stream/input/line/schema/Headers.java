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
package org.talend.components.common.stream.input.line.schema;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class Headers {

    public Schema build(RecordBuilderFactory factory, Iterable<String> fields, boolean isheader) {
        Schema.Builder builder = factory.newSchemaBuilder(Schema.Type.RECORD);
        Set<String> existNames = new HashSet<>();

        int index = 0;
        Iterable<String> realHeaders = findHeaders(fields, isheader);
        for (String header : realHeaders) {
            String finalName = this.getCorrectSchemaFieldName(header, index++, existNames);
            existNames.add(finalName);
            this.addField(factory, builder, finalName);
        }
        return builder.build();
    }

    private void addField(RecordBuilderFactory factory, Schema.Builder builder, String name) {
        final Schema.Entry.Builder entryBuilder = factory.newEntryBuilder();
        builder.withEntry(entryBuilder.withName(name).withType(Schema.Type.STRING).withNullable(true).build());
    }

    /**
     * correct the field name and make it valid for AVRO schema
     * for example :
     * input : "CA HT", output "CA_HT"
     * input : "column?!^Name", output "column___Name"
     * input : "P1_Vente_Qté", output "P1_Vente_Qt_"
     *
     * @param name : the name will be correct
     * @param nameIndex : a index which is used to generate the column name when too much underline in the name
     * @param previousNames : the previous valid names, this is used to make sure that every name is different
     * @return the valid name, if the input name is null or empty, or the previousNames is null, return the input name directly
     */
    private String getCorrectSchemaFieldName(String name, int nameIndex, Set<String> previousNames) {
        if (name == null || name.isEmpty() || previousNames == null) {
            return name;
        }

        StringBuilder str = new StringBuilder();
        int underLineCount = 0;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || ((c >= '0') && (c <= '9') && (i != 0))) {
                str.append(c);
            } else if (c == '_') {
                str.append(c);
                underLineCount++;
            } else {
                str.append('_');
                underLineCount++;
            }
        }

        String result = underLineCount > (name.length() / 2) ? "Column" + nameIndex : str.toString();

        return getUniqueNameForSchemaField(result, previousNames);
    }

    private String getUniqueNameForSchemaField(String name, Set<String> previousNames) {
        int index = 0;
        String currentName = name;
        while (previousNames != null && previousNames.contains(currentName)) {
            currentName = name + (++index);
        }
        return currentName;
    }

    private Iterable<String> findHeaders(final Iterable<String> fields, boolean isHeader) {

        List<String> generateHeaders = new ArrayList<>();
        int i = 1;
        for (String field : fields) {
            if (isHeader) {
                generateHeaders.add(field);
            } else {
                generateHeaders.add("field_" + i);
            }
            i++;
        }
        return generateHeaders;
    }
}
