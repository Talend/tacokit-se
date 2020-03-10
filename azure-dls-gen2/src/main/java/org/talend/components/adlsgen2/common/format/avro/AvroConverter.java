/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.common.format.avro;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.components.adlsgen2.common.format.FileFormatRuntimeException;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.configuration.Configuration;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

@Slf4j
public class AvroConverter implements RecordConverter<GenericRecord>, Serializable {

    public static final String AVRO_LOGICAL_TYPE_DATE = "date";

    public static final String AVRO_LOGICAL_TYPE_TIME_MILLIS = "time-millis";

    public static final String AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";

    public static final String ERROR_UNDEFINED_TYPE = "Undefined type %s.";

    public static final String RECORD_NAME = "talend_";

    public static final String RECORD_NAMESPACE = "org.talend.components.adlsgen2";

    private final AvroConfiguration configuration;

    private RecordBuilderFactory recordBuilderFactory;

    private Schema recordSchema;

    private org.apache.avro.Schema avroSchema;

    public static AvroConverter of(final RecordBuilderFactory factory,
            final @Configuration("avroConfiguration") AvroConfiguration configuration) {
        return new AvroConverter(factory, configuration);
    }

    protected AvroConverter(final RecordBuilderFactory factory,
            final @Configuration("avroConfiguration") AvroConfiguration configuration) {
        recordBuilderFactory = factory;
        this.configuration = configuration;
    }

    @Override
    public Schema inferSchema(GenericRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        record.getSchema().getFields().stream().map(this::inferAvroField).forEach(builder::withEntry);
        return builder.build();
    }

    @Override
    public Record toRecord(GenericRecord record) {
        if (recordSchema == null) {
            recordSchema = inferSchema(record);
        }
        return avroToRecord(record, record.getSchema().getFields(), recordBuilderFactory.newRecordBuilder(recordSchema));
    }

    @Override
    public GenericRecord fromRecord(Record record) {
        if (avroSchema == null) {
            avroSchema = inferAvroSchema(record.getSchema());
        }
        return recordToAvro(record, new GenericData.Record(avroSchema));
    }

    protected GenericRecord recordToAvro(Record fromRecord, GenericRecord toRecord) {
        for (org.apache.avro.Schema.Field f : toRecord.getSchema().getFields()) {
            String name = f.name();
            org.apache.avro.Schema.Type fieldType = getFieldType(f);
            switch (fieldType) {
            case RECORD:
                org.apache.avro.Schema subSchema = inferAvroSchema(fromRecord.getRecord(name).getSchema());
                GenericRecord subrecord = recordToAvro(fromRecord.getRecord(name), new GenericData.Record(subSchema));
                toRecord.put(name, subrecord);
                break;
            case ARRAY:
                Entry e = getSchemaForEntry(name, fromRecord.getSchema());
                Collection<Object> recordArray = fromRecord.getOptionalArray(Object.class, name).orElse(new ArrayList<>());
                if (recordArray.iterator().hasNext()) {
                    Object firstArrayValue = recordArray.iterator().next();
                    if (firstArrayValue instanceof Record) {
                        subSchema = inferAvroSchema(((Record) firstArrayValue).getSchema());
                        List<GenericRecord> records = recordArray.stream()
                                .map(o -> recordToAvro((Record) o, new GenericData.Record(subSchema)))
                                .collect(Collectors.toList());
                        toRecord.put(name, records);
                    } else {
                        toRecord.put(name, fromRecord.getArray(getJavaClassForType(e.getElementSchema().getType()), name));
                    }
                }
                break;
            case STRING:
                toRecord.put(name, fromRecord.getOptionalString(name).orElse(null));
                break;
            case BYTES:
                Optional<byte[]> optionalBytesValue = fromRecord.getOptionalBytes(name);
                if (optionalBytesValue.isPresent()) {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(fromRecord.getBytes(name));
                    toRecord.put(name, byteBuffer);
                } else {
                    toRecord.put(name, null);
                }
                break;
            case INT:
                OptionalInt optionalIntValue = fromRecord.getOptionalInt(name);
                if (optionalIntValue.isPresent()) {
                    toRecord.put(name, optionalIntValue.getAsInt());
                } else {
                    toRecord.put(name, null);
                }
                break;
            case LONG:
                OptionalLong optionalLongValue = fromRecord.getOptionalLong(name);
                if (optionalLongValue.isPresent()) {
                    toRecord.put(name, optionalLongValue.getAsLong());
                } else {
                    toRecord.put(name, null);
                }
                break;
            case FLOAT:
                OptionalDouble optionalFloat = fromRecord.getOptionalFloat(name);
                if (optionalFloat.isPresent()) {
                    toRecord.put(name, Double.valueOf(optionalFloat.getAsDouble()).floatValue());
                } else {
                    toRecord.put(name, null);
                }
                break;
            case DOUBLE:
                OptionalDouble optionalDouble = fromRecord.getOptionalFloat(name);
                if (optionalDouble.isPresent()) {
                    toRecord.put(name, optionalDouble.getAsDouble());
                } else {
                    toRecord.put(name, null);
                }
                break;
            case BOOLEAN:
                toRecord.put(name, fromRecord.getOptionalBoolean(name).orElse(null));
                break;
            }
        }
        return toRecord;
    }

    protected Class<? extends Object> getJavaClassForType(Type type) {
        switch (type) {
        case RECORD:
            return Record.class;
        case ARRAY:
            return Array.class;
        case STRING:
            return String.class;
        case BYTES:
            return Byte[].class;
        case INT:
            return Integer.class;
        case LONG:
            return Long.class;
        case FLOAT:
            return Float.class;
        case DOUBLE:
            return Double.class;
        case BOOLEAN:
            return Boolean.class;
        case DATETIME:
            return ZonedDateTime.class;
        }
        return Object.class;
    }

    protected Entry getSchemaForEntry(String name, Schema schema) {
        for (Entry e : schema.getEntries()) {
            if (name.equals(e.getName())) {
                return e;
            }
        }
        return null;
    }

    protected org.apache.avro.Schema.Type translateToAvroType(Type type) {
        switch (type) {
        case RECORD:
            return org.apache.avro.Schema.Type.RECORD;
        case ARRAY:
            return org.apache.avro.Schema.Type.ARRAY;
        case STRING:
            return org.apache.avro.Schema.Type.STRING;
        case BYTES:
            return org.apache.avro.Schema.Type.BYTES;
        case INT:
            return org.apache.avro.Schema.Type.INT;
        case LONG:
        case DATETIME:
            return org.apache.avro.Schema.Type.LONG;
        case FLOAT:
            return org.apache.avro.Schema.Type.FLOAT;
        case DOUBLE:
            return org.apache.avro.Schema.Type.DOUBLE;
        case BOOLEAN:
            return org.apache.avro.Schema.Type.BOOLEAN;
        }
        throw new FileFormatRuntimeException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
    }

    /**
     * Infer an Avro Schema from a Record Schema
     *
     * @param schema the Record schema
     * @return an Avro Schema
     */
    public org.apache.avro.Schema inferAvroSchema(Schema schema) {
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        for (Entry e : schema.getEntries()) {
            String name = e.getName();
            String comment = e.getComment();
            Object defaultValue = e.getDefaultValue();
            Type type = e.getType();
            org.apache.avro.Schema builder;
            switch (type) {
            case RECORD:
                org.apache.avro.Schema subSchema = inferAvroSchema(e.getElementSchema());
                builder = subSchema;
                break;
            case ARRAY:
                if (e.getElementSchema().getType().equals(Type.RECORD)) {
                    subSchema = inferAvroSchema(e.getElementSchema());
                    builder = org.apache.avro.Schema.createArray(subSchema);
                } else {
                    builder = org.apache.avro.Schema
                            .createArray(org.apache.avro.Schema.create(translateToAvroType(e.getElementSchema().getType())));
                }
                break;
            case STRING:
            case BYTES:
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                builder = org.apache.avro.Schema.create(translateToAvroType(type));
                break;
            case DATETIME:
                builder = org.apache.avro.Schema.create(org.apache.avro.Schema.Type.LONG);
                LogicalTypes.timestampMillis().addToSchema(builder);
                break;
            default:
                throw new FileFormatRuntimeException(String.format(ERROR_UNDEFINED_TYPE, e.getType().name()));
            }
            org.apache.avro.Schema unionWithNull;
            if (builder.getType() == org.apache.avro.Schema.Type.RECORD) {
                unionWithNull = builder;
            } else {
                unionWithNull = SchemaBuilder.unionOf().type(builder).and().nullType().endUnion();
            }
            org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(name, unionWithNull, comment, defaultValue);
            fields.add(field);
        }
        return org.apache.avro.Schema.createRecord(RECORD_NAME + String.valueOf(schema.hashCode()).replace("-", ""), "",
                RECORD_NAMESPACE, false, fields);
    }

    protected Record avroToRecord(GenericRecord genericRecord, List<org.apache.avro.Schema.Field> fields) {
        return avroToRecord(genericRecord, fields, null);
    }

    protected Record avroToRecord(GenericRecord genericRecord, List<org.apache.avro.Schema.Field> fields,
            Record.Builder recordBuilder) {
        if (recordBuilder == null) {
            recordBuilder = recordBuilderFactory.newRecordBuilder(recordSchema);
        }
        for (org.apache.avro.Schema.Field field : fields) {
            Object value = genericRecord.get(field.name());
            Entry entry = inferAvroField(field);
            if (org.apache.avro.Schema.Type.ARRAY.equals(field.schema().getType())) {
                buildArrayField(field, value, recordBuilder, entry);
            } else {
                if (!entry.isNullable() || value != null) {
                    buildField(field, value, recordBuilder, entry);
                }
            }
        }
        return recordBuilder.build();
    }

    protected Entry inferAvroField(org.apache.avro.Schema.Field field) {
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(field.name());
        org.apache.avro.Schema.Type type = getFieldType(field);
        String logicalType = getAvroLogicalTypeName(field);
        // handle NULLable field
        builder.withNullable(true);
        switch (type) {
        case RECORD:
            builder.withType(Type.RECORD);
            //
            Schema.Builder subBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
            org.apache.avro.Schema extractedSchema = getUnionSchema(field.schema());
            extractedSchema.getFields().stream().map(this::inferAvroField).forEach(subBuilder::withEntry);
            builder.withElementSchema(subBuilder.build());
            break;
        case ENUM:
        case ARRAY:
            builder.withType(Type.ARRAY);
            extractedSchema = getUnionSchema(getUnionSchema(field.schema()).getElementType());
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

    /**
     *
     */
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
            throw new FileFormatRuntimeException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void buildArrayField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder, Entry entry) {
        org.apache.avro.Schema arraySchema = getUnionSchema(field.schema());
        switch (arraySchema.getElementType().getType()) {
        case RECORD:
            Collection<Record> recs = ((Collection<GenericRecord>) value).stream()
                    .map(record -> avroToRecord(record, record.getSchema().getFields(), recordBuilderFactory.newRecordBuilder()))
                    .collect(toList());
            recordBuilder.withArray(entry, recs);
            break;
        case STRING:
            recordBuilder.withArray(entry, (Collection<String>) value);
            break;
        case BYTES:
            recordBuilder.withArray(entry, ((Collection<Byte[]>) value));
            break;
        case INT:
            recordBuilder.withArray(entry, (Collection<Integer>) value);
            break;
        case LONG:
            recordBuilder.withArray(entry, (Collection<Long>) value);
            break;
        case FLOAT:
            recordBuilder.withArray(entry, (Collection<Float>) value);
            break;
        case DOUBLE:
            recordBuilder.withArray(entry, (Collection<Double>) value);
            break;
        case BOOLEAN:
            recordBuilder.withArray(entry, (Collection<Boolean>) value);
            break;
        case NULL:
            break;
        default:
            throw new FileFormatRuntimeException(String.format(ERROR_UNDEFINED_TYPE, entry.getType().name()));
        }
    }

    protected void buildField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder, Entry entry) {
        String logicalType = getAvroLogicalTypeName(field);
        org.apache.avro.Schema.Type fieldType = getFieldType(field);
        switch (fieldType) {
        case RECORD:
            recordBuilder.withRecord(entry, avroToRecord((GenericRecord) value, ((GenericRecord) value).getSchema().getFields(),
                    recordBuilderFactory.newRecordBuilder()));
            break;
        case ARRAY:
            buildArrayField(field, value, recordBuilder, entry);
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
            throw new FileFormatRuntimeException(String.format(ERROR_UNDEFINED_TYPE, entry.getType().name()));
        }
    }

    private String getAvroLogicalTypeName(org.apache.avro.Schema.Field field) {
        return getUnionSchema(field.schema()).getLogicalType() == null ? ""
                : getUnionSchema(field.schema()).getLogicalType().getName();
    }

    private org.apache.avro.Schema getUnionSchema(org.apache.avro.Schema inputSchema) {
        org.apache.avro.Schema elementType;
        if (inputSchema.getType() == org.apache.avro.Schema.Type.UNION) {
            List<org.apache.avro.Schema> extractedSchemas = inputSchema.getTypes().stream()
                    .filter(schema -> !schema.getType().equals(org.apache.avro.Schema.Type.NULL)).collect(toList());
            // should have only one schema element with nullable (UNION)
            elementType = extractedSchemas.get(0);
        } else {
            elementType = inputSchema;
        }
        return elementType;
    }

    private org.apache.avro.Schema.Type getFieldType(org.apache.avro.Schema.Field field) {
        return getUnionSchema(field.schema()).getType();
    }

}
