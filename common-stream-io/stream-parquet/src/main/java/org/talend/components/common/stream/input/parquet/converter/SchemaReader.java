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

import java.util.List;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.talend.components.common.stream.format.parquet.Constants;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.sdk.component.api.record.Schema;
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
        final Name name = Name.fromParquetName(parquetField.getName());
        final Schema.Entry.Builder entryBuilder = this.factory.newEntryBuilder() //
                .withName(name.getName()) //
                .withRawName(name.getRawName());
        if (parquetField.isRepetition(Repetition.REPEATED) || this.isArrayEncapsulated(parquetField)) {
            final Type innerArrayType = TCKConverter.innerArrayType(parquetField);
            entryBuilder.withNullable(true);
            entryBuilder.withType(Schema.Type.ARRAY);
            if (innerArrayType == null) {
                if (parquetField.isPrimitive()) {
                    final Schema.Type tckType = TCKPrimitiveTypes.toTCKType(parquetField.asPrimitiveType());
                    entryBuilder.withElementSchema(factory.newSchemaBuilder(tckType).build());
                } else {
                    final Schema schema = this.extractRecordType(parquetField.asGroupType());
                    entryBuilder.withElementSchema(schema);
                }
            } else if (innerArrayType.isPrimitive()) {
                final Schema.Type tckType = TCKPrimitiveTypes.toTCKType(innerArrayType.asPrimitiveType());
                entryBuilder.withElementSchema(this.factory.newSchemaBuilder(tckType).build());
            } else {
                final Schema schema = this.extractRecordType(innerArrayType.asGroupType());
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

    private boolean isArrayEncapsulated(final org.apache.parquet.schema.Type parquetField) {
        if (parquetField.isPrimitive()) {
            return false;
        }

        final List<org.apache.parquet.schema.Type> fields = parquetField.asGroupType().getFields();
        if (fields != null && fields.size() == 1) {
            final org.apache.parquet.schema.Type elementType = fields.get(0);
            if (Constants.LIST_NAME.equals(elementType.getName())) {
                return elementType.getRepetition() == Repetition.REPEATED;
            }
        }
        return false;
    }

    private Schema extractRecordType(final GroupType gt) {

        final List<org.apache.parquet.schema.Type> fields = gt.getFields();
        if (fields != null && fields.size() == 1) {
            final org.apache.parquet.schema.Type listType = fields.get(0);
            if (listType != null && Constants.LIST_NAME.equals(listType.getName()) && (!listType.isPrimitive())) {
                final List<Type> listFields = listType.asGroupType().getFields();
                if (listFields != null && listFields.size() == 1) {
                    final org.apache.parquet.schema.Type elementType = listFields.get(0);
                    if (elementType.isPrimitive()) {
                        return extractArrayType(elementType.asPrimitiveType());
                    }
                    final GroupType groupType = elementType.asGroupType();
                    final List<Type> groupTypeFields = groupType.getFields();
                    if (groupTypeFields != null && groupTypeFields.size() == 1) {
                        final org.apache.parquet.schema.Type innerType = groupTypeFields.get(0);
                        if ((!innerType.isPrimitive()) && Constants.LIST_NAME.equals(listType.getName())) {
                            final Schema sub = this.extractRecordType(innerType.asGroupType());
                            return this.factory.newSchemaBuilder(Schema.Type.ARRAY).withElementSchema(sub).build();
                        }
                    }
                    final Schema sub = this.extractRecordType(groupType);
                    return this.factory.newSchemaBuilder(Schema.Type.ARRAY).withElementSchema(sub).build();
                }
            }
        }

        final Schema.Builder builder = this.factory.newSchemaBuilder(Schema.Type.RECORD);
        if (fields != null) {
            fields.stream() //
                    .map(this::extractTCKField) // get tck entry
                    .forEach(builder::withEntry); // into schema.
        }
        return builder.build();
    }

    private Schema extractArrayType(PrimitiveType parquetType) {
        final Schema.Builder builder = this.factory.newSchemaBuilder(Schema.Type.ARRAY);
        final Schema.Type tckType = TCKPrimitiveTypes.toTCKType(parquetType);
        return builder.withElementSchema(this.factory.newSchemaBuilder(tckType).build()).build();
    }

}
