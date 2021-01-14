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
package org.talend.components.common.stream.output.avro;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema.Field;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericRecord;
import org.talend.components.common.stream.AvroHelper;
import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;

public class RecordToAvro implements RecordConverter<GenericRecord, org.apache.avro.Schema> {

    private static final String ERROR_UNDEFINED_TYPE = "Undefined type %s.";

    private static final String AVRO_PROP_JAVA_CLASS = "java-class";

    private static final String AVRO_PROP_TALEND_FIELD_PATTERN = "talend.field.pattern";

    private static final String RECORD_NAME = "talend_";

    private org.apache.avro.Schema avroSchema;

    private final String currentRecordNamespace;

    public RecordToAvro(String currentRecordNamespace) {
        assert currentRecordNamespace != null : "currentRecordNamespace can't be null";
        this.currentRecordNamespace = currentRecordNamespace;
    }

    @Override
    public GenericRecord fromRecord(Record record) {
        if (avroSchema == null) {
            avroSchema = fromRecordSchema(record.getSchema());
        }
        return recordToAvro(record, new GenericData.Record(avroSchema));
    }

    private GenericRecord recordToAvro(Record fromRecord, GenericRecord toRecord) {
        if (fromRecord == null) {
            return toRecord;
        }
        for (org.apache.avro.Schema.Field f : toRecord.getSchema().getFields()) {
            final String name = f.name();
            final org.apache.avro.Schema.Type fieldType = AvroHelper.getFieldType(f);
            switch (fieldType) {
            case RECORD:
                final Record record = fromRecord.getRecord(name);
                final Schema schema = fromRecord.getSchema().getEntries().stream().filter(e -> name.equals(e.getName()))
                        .findFirst().map(Entry::getElementSchema).orElse(null);
                if (record != null) {
                    final org.apache.avro.Schema subSchema = fromRecordSchema(record.getSchema());
                    final GenericRecord subrecord = recordToAvro(record, new GenericData.Record(subSchema));
                    toRecord.put(name, subrecord);
                }
                break;
            case ARRAY:
                final Entry e = getSchemaForEntry(name, fromRecord.getSchema());
                final Collection<Object> recordArray = fromRecord.getOptionalArray(Object.class, name).orElse(new ArrayList<>());
                if (!recordArray.isEmpty()) {
                    final Object firstArrayValue = recordArray.iterator().next();
                    if (firstArrayValue instanceof Record) {
                        final org.apache.avro.Schema subSchema = fromRecordSchema(((Record) firstArrayValue).getSchema());
                        final List<GenericRecord> records = recordArray.stream()
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
                final Optional<byte[]> optionalBytesValue = fromRecord.getOptionalBytes(name);
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
                OptionalDouble optionalDouble = fromRecord.getOptionalDouble(name);
                if (optionalDouble.isPresent()) {
                    toRecord.put(name, optionalDouble.getAsDouble());
                } else {
                    toRecord.put(name, null);
                }
                break;
            case BOOLEAN:
                toRecord.put(name, fromRecord.getOptionalBoolean(name).orElse(null));
                break;
            default:
                throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, fieldType.name()));
            }
        }

        return toRecord;
    }

    /**
     * Infer an Avro Schema from a Record Schema
     *
     * @param schema the Record schema
     * @return an Avro Schema
     */
    @Override
    public org.apache.avro.Schema fromRecordSchema(Schema schema) {
        List<Field> fields = new ArrayList<>();
        for (Entry e : schema.getEntries()) {
            String name = e.getName();
            String comment = e.getComment();
            Object defaultValue = e.getDefaultValue();
            Type type = e.getType();
            org.apache.avro.Schema builder;
            switch (type) {
            case RECORD:
                builder = fromRecordSchema(e.getElementSchema());
                break;
            case ARRAY:
                org.apache.avro.Schema subschema = fromRecordSchema(e.getElementSchema());
                builder = org.apache.avro.Schema.createArray(subschema);
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
                builder.addProp(AVRO_PROP_TALEND_FIELD_PATTERN, ""); // for studio
                builder.addProp(AVRO_PROP_JAVA_CLASS, Date.class.getCanonicalName()); // for studio
                break;
            default:
                throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, e.getType().name()));
            }
            org.apache.avro.Schema unionWithNull;
            if (builder.getType() == org.apache.avro.Schema.Type.RECORD && (!e.isNullable())) {
                unionWithNull = builder;
            } else {
                unionWithNull = SchemaBuilder.unionOf().type(builder).and().nullType().endUnion();
            }
            org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(name, unionWithNull, comment, defaultValue);
            fields.add(field);
        }
        return org.apache.avro.Schema.createRecord(this.buildSchemaId(schema), "", currentRecordNamespace, false, fields);
    }

    /**
     * Build an id that is same for equivalent schema independently of implementation.
     * 
     * @param schema : schema.
     * @return id
     */
    private String buildSchemaId(Schema schema) {
        final List<String> fields = schema.getEntries().stream()
                .map((Entry e) -> e.getName() + "_" + e.getType() + e.isNullable()).collect(Collectors.toList());
        return (RECORD_NAME + fields.hashCode()).replace('-', '1');
    }

    private org.apache.avro.Schema.Type translateToAvroType(final Type type) {
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
        throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
    }

    private Entry getSchemaForEntry(String name, Schema schema) {
        for (Entry e : schema.getEntries()) {
            if (name.equals(e.getName())) {
                return e;
            }
        }
        return null;
    }

    private Class<?> getJavaClassForType(Schema.Type type) {
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
}
