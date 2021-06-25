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
package org.talend.components.common.stream.output.parquet.converter;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Type.Repetition;
import org.apache.parquet.schema.Types;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.talend.components.common.stream.format.parquet.Constants;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.sdk.component.api.record.Schema;

public class SchemaWriter {

    public MessageType convert(final Schema tckSchema) {
        final List<org.apache.parquet.schema.Type> fields = this.extractTypes(tckSchema.getEntries());
        final MessageType mt = new MessageType("RECORD", fields);

        return mt;
    }

    private List<org.apache.parquet.schema.Type> extractTypes(List<Schema.Entry> entries) {
        return entries.stream() //
                .map(this::toParquetType) //
                .collect(Collectors.toList());
    }

    private GroupType convert(Type.Repetition repetition, final String name, Schema schema) {
        if (schema.getType() == Schema.Type.ARRAY) {
            final Schema elementSchema = schema.getElementSchema();
            if (elementSchema.getType() == Schema.Type.ARRAY || elementSchema.getType() == Schema.Type.RECORD) {
                final GroupType convert = this.convert(Repetition.OPTIONAL, Constants.ELEMENT_NAME, elementSchema);
                return ConversionPatterns.listOfElements(Repetition.REPEATED, name, convert);
            } else {
                final PrimitiveType primitiveType = this.toPrimitive(Repetition.OPTIONAL, Constants.ELEMENT_NAME,
                        elementSchema.getType());
                return ConversionPatterns.listOfElements(Repetition.REPEATED, name, primitiveType);
            }
        } else if (schema.getType() == Schema.Type.RECORD) {
            final List<org.apache.parquet.schema.Type> fields = this.extractTypes(schema.getEntries());
            return new GroupType(repetition, name, fields);
        }
        return null;
    }

    private PrimitiveType toPrimitive(Type.Repetition repetition, final String name, Schema.Type tckType) {

        final PrimitiveTypeName primitiveTypeName = ParquetPrimitiveTypes.toParquetType(tckType);
        final PrimitiveBuilder<PrimitiveType> primitive = Types.primitive(primitiveTypeName, repetition);
        if (tckType == Schema.Type.STRING) {
            primitive.as(LogicalTypeAnnotation.stringType());
        } else if (tckType == Schema.Type.DATETIME) {
            primitive.as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
        }
        return primitive.named(name);
    }

    private org.apache.parquet.schema.Type toParquetType(final Schema.Entry field) {
        final Name fname = new Name(field.getName(), field.getRawName());
        final Repetition repetition = this.getRepetition(field);

        final org.apache.parquet.schema.Type parquetType;
        if (field.getType() == Schema.Type.RECORD) {
            parquetType = this.convert(repetition, fname.parquetName(), field.getElementSchema());
        } else if (field.getType() == Schema.Type.ARRAY) {
            org.apache.parquet.schema.Type innerType = this.convert(repetition, Constants.ELEMENT_NAME, field.getElementSchema());
            if (innerType == null) {
                innerType = this.toPrimitive(repetition, Constants.ELEMENT_NAME, field.getElementSchema().getType());
            }
            // final GroupType convert = new GroupType(Repetition.OPTIONAL, Constants.ELEMENT_NAME, innerType);
            parquetType = ConversionPatterns.listOfElements(Repetition.REPEATED, fname.parquetName(), innerType);
        } else {
            parquetType = this.toPrimitive(repetition, fname.parquetName(), field.getType());
        }
        return parquetType;
    }

    private Type.Repetition getRepetition(final Schema.Entry field) {
        if (field.getType() == Schema.Type.ARRAY) {
            return Repetition.REPEATED;
        }
        if (field.isNullable()) {
            return Repetition.OPTIONAL;
        }
        return Repetition.REQUIRED;
    }

}
