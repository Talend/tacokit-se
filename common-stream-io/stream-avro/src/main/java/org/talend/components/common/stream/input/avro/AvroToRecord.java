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
package org.talend.components.common.stream.input.avro;

import static java.util.stream.Collectors.toList;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.avro.generic.GenericRecord;
import org.talend.components.common.stream.AvroHelper;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class AvroToRecord {

    private static final String AVRO_LOGICAL_TYPE = "logicalType";

    private static final String AVRO_LOGICAL_TYPE_DATE = "date";

    private static final String AVRO_LOGICAL_TYPE_TIME_MILLIS = "time-millis";

    private static final String AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";

    private static final String ERROR_UNDEFINED_TYPE = "Undefined type %s.";

    private final RecordBuilderFactory recordBuilderFactory;

    private Schema recordSchema;

    public AvroToRecord(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    public Schema inferSchema(GenericRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        record.getSchema().getFields().stream().map(this::inferAvroField).forEach(builder::withEntry);
        return builder.build();
    }

    public Record toRecord(GenericRecord record) {
        if (recordSchema == null) {
            recordSchema = inferSchema(record);
        }
        return avroToRecord(record, record.getSchema().getFields(), recordBuilderFactory.newRecordBuilder(recordSchema));
    }

    private Record avroToRecord(GenericRecord genericRecord, List<org.apache.avro.Schema.Field> fields,
            Record.Builder recordBuilder) {
        if (recordBuilder == null) {
            recordBuilder = recordBuilderFactory.newRecordBuilder(recordSchema);
        }
        for (org.apache.avro.Schema.Field field : fields) {
            Object value = genericRecord.get(field.name());
            Entry entry = inferAvroField(field);
            if (org.apache.avro.Schema.Type.ARRAY.equals(field.schema().getType()) && value instanceof Collection) {
                buildArrayField(field, (Collection<?>) value, recordBuilder, entry);
            } else if (!entry.isNullable() || value != null) {
                buildField(field, value, recordBuilder, entry);
            }

        }
        return recordBuilder.build();
    }

    private Entry inferAvroField(org.apache.avro.Schema.Field field) {
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(field.name());
        org.apache.avro.Schema.Type type = AvroHelper.getFieldType(field);
        String logicalType = field.schema().getProp(AVRO_LOGICAL_TYPE);
        // handle NULLable field
        builder.withNullable(true);
        switch (type) {
        case RECORD:
            builder.withType(Type.RECORD);
            //
            Schema.Builder subBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            org.apache.avro.Schema extractedSchema = AvroHelper.getUnionSchema(field.schema());
            extractedSchema.getFields().stream().map(this::inferAvroField).forEach(subBuilder::withEntry);
            builder.withElementSchema(subBuilder.build());
            break;
        case ENUM:
        case ARRAY:
            builder.withType(Type.ARRAY);
            extractedSchema = AvroHelper.getUnionSchema(AvroHelper.getUnionSchema(field.schema()).getElementType());
            Type toType = translateToRecordType((extractedSchema.getType()));
            subBuilder = recordBuilderFactory.newSchemaBuilder(toType);
            switch (toType) {
            case RECORD:
            case ARRAY:
                extractedSchema.getFields().stream().map(this::inferAvroField).forEach(subBuilder::withEntry);
                builder.withElementSchema(subBuilder.build());
                break;
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
            case DATETIME:
                builder.withElementSchema(subBuilder.build());
                break;
            }
            break;
        case INT:
        case LONG:
            if (AVRO_LOGICAL_TYPE_DATE.equals(logicalType) || AVRO_LOGICAL_TYPE_TIME_MILLIS.equals(logicalType)
                    || AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType)) {
                builder.withType(Schema.Type.DATETIME);
                break;
            }
        case STRING:
        case BYTES:
        case FLOAT:
        case DOUBLE:
        case BOOLEAN:
        case NULL:
            builder.withType(translateToRecordType(type));
            break;
        }
        return builder.build();
    }

    private void buildArrayField(org.apache.avro.Schema.Field field, Collection<?> value, Record.Builder recordBuilder,
            Entry entry) {
        final org.apache.avro.Schema arraySchema = AvroHelper.getUnionSchema(field.schema());
        final org.apache.avro.Schema arrayInnerType = arraySchema.getElementType();

        final Collection<?> objectArray;
        switch (arrayInnerType.getType()) {
        case RECORD:
            objectArray = ((Collection<GenericRecord>) value).stream()
                    .map(record -> avroToRecord(record, arrayInnerType.getFields(), recordBuilderFactory.newRecordBuilder()))
                    .collect(Collectors.toList());
            break;
        case STRING:
            objectArray = value.stream().map(Object::toString).collect(Collectors.toList());
            break;
        case BYTES:
            objectArray = ((Collection<ByteBuffer>) value).stream().map(ByteBuffer::array).collect(toList());
            break;
        case INT:
            objectArray = ((Collection<Integer>) value).stream().collect(toList());
            break;
        case FLOAT:
            objectArray = ((Collection<Float>) value).stream().collect(toList());
            break;
        case DOUBLE:
            objectArray = ((Collection<Double>) value).stream().collect(toList());
            break;
        case BOOLEAN:
            objectArray = ((Collection<Boolean>) value).stream().collect(toList());
            break;
        case LONG:
            objectArray = ((Collection<Long>) value).stream().collect(toList());
            break;
        default:
            throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, entry.getType().name()));
        }
        recordBuilder.withArray(entry, objectArray);
    }

    protected void buildField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder, Entry entry) {
        String logicalType = field.schema().getProp(AVRO_LOGICAL_TYPE);
        org.apache.avro.Schema.Type fieldType = AvroHelper.getFieldType(field);
        switch (fieldType) {
        case RECORD:
            recordBuilder.withRecord(entry, avroToRecord((GenericRecord) value, ((GenericRecord) value).getSchema().getFields(),
                    recordBuilderFactory.newRecordBuilder()));
            break;
        case ARRAY:
            if (value instanceof Collection<?>) {
                buildArrayField(field, (Collection<?>) value, recordBuilder, entry);
            }
            break;
        case STRING:
            recordBuilder.withString(entry, value != null ? value.toString() : null);
            break;
        case BYTES:
            byte[] bytes = value != null ? ((java.nio.ByteBuffer) value).array() : null;
            recordBuilder.withBytes(entry, bytes);
            break;
        case INT:
            int ivalue = value != null ? (Integer) value : 0;
            if (AVRO_LOGICAL_TYPE_DATE.equals(logicalType) || AVRO_LOGICAL_TYPE_TIME_MILLIS.equals(logicalType)) {
                recordBuilder.withDateTime(entry, ZonedDateTime.ofInstant(Instant.ofEpochMilli(ivalue), ZoneOffset.UTC));
            } else {
                recordBuilder.withInt(entry, ivalue);
            }
            break;
        case FLOAT:
            recordBuilder.withFloat(entry, value != null ? (Float) value : 0);
            break;
        case DOUBLE:
            recordBuilder.withDouble(entry, value != null ? (Double) value : 0);
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(entry, value != null ? (Boolean) value : Boolean.FALSE);
            break;
        case LONG:
            long lvalue = value != null ? (Long) value : 0;
            if (AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS.equals(logicalType)) {
                recordBuilder.withDateTime(entry, ZonedDateTime.ofInstant(Instant.ofEpochMilli(lvalue), ZoneOffset.UTC));
            } else {
                recordBuilder.withLong(entry, lvalue);
            }
            break;
        default:
            throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, entry.getType().name()));
        }
    }

    protected Type translateToRecordType(org.apache.avro.Schema.Type type) {
        switch (type) {
        case RECORD:
            return Type.RECORD;
        case ARRAY:
            return Type.ARRAY;
        case STRING:
            return Type.STRING;
        case BYTES:
            return Type.BYTES;
        case INT:
            return Type.INT;
        case LONG:
            return Type.LONG;
        case FLOAT:
            return Type.FLOAT;
        case DOUBLE:
            return Type.DOUBLE;
        case BOOLEAN:
            return Type.BOOLEAN;
        default:
            throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
        }
    }
}
