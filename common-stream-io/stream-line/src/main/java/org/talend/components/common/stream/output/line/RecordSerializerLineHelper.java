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
package org.talend.components.common.stream.output.line;

import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;

import org.talend.components.common.stream.SchemaHelper;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class RecordSerializerLineHelper {

    public static List<String> valuesFrom(Record record) {
        final List<String> result = new ArrayList<>();

        for (Entry entry : record.getSchema().getEntries()) {
            if (entry.getType() == Schema.Type.RECORD) {
                record.getOptionalRecord(entry.getName()).map(RecordSerializerLineHelper::valuesFrom)
                        .orElse(Collections.emptyList()).forEach(result::add);
            } else if (entry.getType() == Type.ARRAY) {
                // can't translate array to Line.
                log.warn("Can't translate array in line format ({}).", entry.getName());
            } else if (entry.getType() == Type.BYTES) {
                final String value = record.getOptionalBytes(entry.getName()) //
                        .map(Base64.getEncoder()::encodeToString) //
                        .orElse(null);
                result.add(value);
            } else {
                final Object obj = record.get(SchemaHelper.getFrom(entry.getType()), entry.getName());
                result.add(obj == null ? null : obj.toString());
            }
        }
        return result;
    }

    public static List<String> schemaFrom(Schema schema) {
        final List<String> result = new ArrayList<>();
        for (Entry entry : schema.getEntries()) {
            if (entry.getType() == Schema.Type.RECORD) {
                final List<String> subSchema = RecordSerializerLineHelper.schemaFrom(entry.getElementSchema());
                subSchema.stream().map((String name) -> entry.getName() + "." + name).forEach(result::add);
            } else if (entry.getType() == Type.ARRAY) {
                // can't translate array to Line.
                log.warn("Can't translate array in line format ({})", entry.getName());
            } else {
                result.add(entry.getName());
            }
        }
        return result;
    }
}
