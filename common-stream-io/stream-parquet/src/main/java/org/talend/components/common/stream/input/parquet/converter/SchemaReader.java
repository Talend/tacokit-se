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
package org.talend.components.common.stream.input.parquet.converter;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.Type.Repetition;
import org.talend.components.common.stream.input.parquet.converter.TCKPrimitiveTypes;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SchemaReader {

    private final RecordBuilderFactory factory;

    public Schema convert(final GroupType msg) {
        return this.extractRecordType(msg);
    }

    /**
     * Extract field from parquet field.
     * 
     * @param parquetField : field of parquert group type.
     * @return
     */
    private Schema.Entry extractTCKField(final org.apache.parquet.schema.Type parquetField) {
        final Schema.Entry.Builder entryBuilder = this.factory.newEntryBuilder().withName(parquetField.getName());
        if (parquetField.getRepetition() == Repetition.REPEATED) {
            entryBuilder.withNullable(true);
            entryBuilder.withType(Schema.Type.ARRAY);
            if (parquetField.isPrimitive()) {
                final Schema.Type tckType = TCKPrimitiveTypes.toTCKType(parquetField.asPrimitiveType());
                entryBuilder.withElementSchema(this.factory.newSchemaBuilder(tckType).build());
            } else {
                final Schema schema = this.extractRecordType(parquetField.asGroupType());
                entryBuilder.withElementSchema(schema);
            }
        } else {
            if (parquetField.isPrimitive()) {
                final Schema.Type tckType = TCKPrimitiveTypes.toTCKType(parquetField.asPrimitiveType());
                entryBuilder.withType(tckType);
            } else {
                final Schema schema = this.extractRecordType(parquetField.asGroupType());
                entryBuilder.withType(Schema.Type.RECORD);
                entryBuilder.withElementSchema(schema);
            }
            entryBuilder.withNullable(parquetField.getRepetition() == Repetition.OPTIONAL);
        }
        return entryBuilder.build();
    }

    private Schema extractRecordType(final GroupType gt) {
        final Schema.Builder builder = this.factory.newSchemaBuilder(Type.RECORD);

        gt.getFields().stream() //
                .map(this::extractTCKField) // get tck entry
                .forEach(builder::withEntry); // into schema.

        return builder.build();
    }

}
