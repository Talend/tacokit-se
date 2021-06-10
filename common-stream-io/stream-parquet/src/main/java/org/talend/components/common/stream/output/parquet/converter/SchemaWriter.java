package org.talend.components.common.stream.output.parquet.converter;

import java.util.List;
import java.util.stream.Collectors;

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
import org.talend.components.common.stream.output.parquet.converter.ParquetPrimitiveTypes;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

public class SchemaWriter {

    public MessageType convert(final Schema tckSchema) {
        final List<org.apache.parquet.schema.Type> fields = this.extractTypes(tckSchema.getEntries());
        return new MessageType("RECORD", fields);
    }

    private List<org.apache.parquet.schema.Type> extractTypes(List<Entry> entries) {
        return entries.stream() //
                .map(this::toParquetType) //
                .collect(Collectors.toList());
    }

    private GroupType convert(Type.Repetition repetition, final String name, Schema schema) {
        final List<org.apache.parquet.schema.Type> fields = this.extractTypes(schema.getEntries());
        return new GroupType(repetition, name, fields);
    }

    private PrimitiveType toPrimitive(Type.Repetition repetition, final String name, Schema.Type tckType) {

        final PrimitiveTypeName primitiveTypeName = ParquetPrimitiveTypes.toParquetType(tckType);
        final PrimitiveBuilder<PrimitiveType> primitive = Types.primitive(primitiveTypeName, repetition);
        if (tckType == Schema.Type.STRING) {
            primitive.as(LogicalTypeAnnotation.stringType());
        }
        else if (tckType == Schema.Type.DATETIME) {
            primitive.as(LogicalTypeAnnotation.timestampType(true, TimeUnit.MILLIS));
        }
        return primitive.named(name);
    }

    private org.apache.parquet.schema.Type toParquetType(final Schema.Entry field) {
        if (field.getType() == Schema.Type.ARRAY) {
            final Schema elementSchema = field.getElementSchema();
            if (elementSchema.getType() == Schema.Type.RECORD) {
                return this.convert(Repetition.REPEATED, field.getOriginalFieldName(), elementSchema);
            }
            else if (elementSchema.getType() == Schema.Type.ARRAY) {
                final Schema subArray = elementSchema.getElementSchema();
                final org.apache.parquet.schema.Type arrayType;
                if (subArray.getType() == Schema.Type.ARRAY || subArray.getType() == Schema.Type.RECORD) {
                    arrayType = this.convert(Repetition.REPEATED, "element", subArray);
                }
                else {
                    arrayType = this.toPrimitive(Repetition.REPEATED, "element", subArray.getType());
                }
                return new GroupType(Repetition.REPEATED, field.getName(), arrayType);

            }
            else {
                return this.toPrimitive(Repetition.REPEATED, field.getName(), elementSchema.getType());
            }
        }
        if (field.getType()  == Schema.Type.RECORD) {
            return this.convert(this.getRepetition(field), field.getName(), field.getElementSchema());
        }
        return this.toPrimitive(this.getRepetition(field), field.getName(), field.getType());
    }

    private Type.Repetition getRepetition(final Entry field) {
        if (field.getType() == Schema.Type.ARRAY) {
            return Repetition.REPEATED;
        }
        if (field.isNullable()) {
            return Repetition.OPTIONAL;
        }
        return Repetition.REQUIRED;
    }

}
