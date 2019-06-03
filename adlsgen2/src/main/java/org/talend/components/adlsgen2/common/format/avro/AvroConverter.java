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
package org.talend.components.adlsgen2.common.format.avro;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.talend.components.adlsgen2.common.converter.RecordConverter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.toList;

@Slf4j
public class AvroConverter implements RecordConverter<GenericRecord> {

    public static final String AVRO_LOGICAL_TYPE = "logicalType";

    public static final String AVRO_LOGICAL_TYPE_DATE = "date";

    public static final String AVRO_LOGICAL_TYPE_TIME_MILLIS = "time-millis";

    public static final String AVRO_LOGICAL_TYPE_TIMESTAMP_MILLIS = "timestamp-millis";

    public static final String AVRO_PROP_JAVA_CLASS = "java-class";

    public static final String AVRO_PROP_TALEND_FIELD_PATTERN = "talend.field.pattern";

    public static final String ERROR_UNDEFINED_TYPE = "Undefined type %s.";

    public static final String RECORD_NAME = "talend_";

    public static final String RECORD_NAMESPACE = "org.talend.components.adlsgen2";

    private RecordBuilderFactory recordBuilderFactory;

    private Schema recordSchema;

    private org.apache.avro.Schema avroSchema;

    public static AvroConverter of(RecordBuilderFactory factory) {
        return new AvroConverter(factory);
    }

    protected AvroConverter(RecordBuilderFactory factory) {
        recordBuilderFactory = factory;
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
            switch (f.schema().getType()) {
            case RECORD:
                toRecord.put(name, fromRecord.getRecord(name));
                break;
            case ARRAY:
                Entry e = getSchemaForEntry(name, fromRecord.getSchema());
                if (e != null) {
                    toRecord.put(name, fromRecord.getArray(getJavaClassForType(e.getElementSchema().getType()), name));
                }
                break;
            case STRING:
                toRecord.put(name, fromRecord.getString(name));
                break;
            case BYTES:
                toRecord.put(name, fromRecord.getBytes(name));
                break;
            case INT:
                toRecord.put(name, fromRecord.getInt(name));
                break;
            case LONG:
                toRecord.put(name, fromRecord.getLong(name));
                break;
            case FLOAT:
                toRecord.put(name, fromRecord.getFloat(name));
                break;
            case DOUBLE:
                toRecord.put(name, fromRecord.getDouble(name));
                break;
            case BOOLEAN:
                toRecord.put(name, fromRecord.getBoolean(name));
                break;
            }
        }
        return toRecord;
    }

    protected Class<? extends Object> getJavaClassForType(Type type) {
        switch (type) {
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
        throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
    }

    /**
     * Infer an Avro Schema from a Record Schema
     *
     * @param schema the Record schema
     * @return an Avro Schema
     */
    protected org.apache.avro.Schema inferAvroSchema(Schema schema) {
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        for (Entry e : schema.getEntries()) {
            String name = e.getName();
            String comment = e.getComment();
            Object defaultValue = e.getDefaultValue();
            Type type = e.getType();
            org.apache.avro.Schema builder;
            switch (type) {
            case RECORD:
                builder = inferAvroSchema(e.getElementSchema());
                break;
            case ARRAY:
                builder = SchemaBuilder.array()
                        .items(org.apache.avro.Schema.create(translateToAvroType(e.getElementSchema().getType())));
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
                builder.addProp(AVRO_PROP_TALEND_FIELD_PATTERN, ""); // mainly for studio
                builder.addProp(AVRO_PROP_JAVA_CLASS, Date.class.getCanonicalName()); // mainly for studio
                break;
            default:
                throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, e.getType().name()));
            }
            org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(name, builder, comment, defaultValue);
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
                buildField(field, value, recordBuilder, entry);
            }
        }
        return recordBuilder.build();
    }

    protected Entry inferAvroField(org.apache.avro.Schema.Field field) {
        Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(field.name());
        org.apache.avro.Schema.Type type = field.schema().getType();
        String logicalType = field.schema().getProp(AVRO_LOGICAL_TYPE);
        // handle NULLable field
        if (org.apache.avro.Schema.Type.UNION.equals(type)) {
            List<org.apache.avro.Schema.Type> tt = field.schema().getTypes().stream().map(org.apache.avro.Schema::getType)
                    .filter(t -> !t.equals(org.apache.avro.Schema.Type.NULL)).collect(toList());
            if (tt.size() == 0 || tt.size() > 1) {
                throw new IllegalStateException("[inferAvroField] Problem with UNION: Cannot determine Type.");
            }
            type = tt.get(0);
        }
        builder.withNullable(true);
        switch (type) {
        case RECORD:
            builder.withType(Type.RECORD);
            builder.withElementSchema(buildRecordFieldSchema(field));
            break;
        case ENUM:
        case ARRAY:
            builder.withType(Type.ARRAY);
            builder.withElementSchema(buildArrayFieldSchema(field));
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

    protected Schema buildRecordFieldSchema(org.apache.avro.Schema.Field field) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        field.schema().getFields().stream().map(this::inferAvroField).forEach(builder::withEntry);
        return builder.build();
    }

    protected Schema buildArrayFieldSchema(org.apache.avro.Schema.Field field) {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Type.RECORD);
        Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        entryBuilder.withName(field.name());
        entryBuilder.withType(translateToRecordType(field.schema().getElementType().getType()));
        schemaBuilder.withEntry(entryBuilder.build());
        return schemaBuilder.build();
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
            throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, type.name()));
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void buildArrayField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder, Entry entry) {
        switch (field.schema().getElementType().getType()) {
        case RECORD:
            recordBuilder.withArray(entry, ((GenericData.Array<GenericRecord>) value).stream()
                    .map(record -> avroToRecord(record, field.schema().getFields())).collect(toList()));
            break;
        case STRING:
            recordBuilder.withArray(entry, (List<String>) value);
            break;
        case BYTES:
            recordBuilder.withArray(entry,
                    ((GenericData.Array<ByteBuffer>) value).stream().map(ByteBuffer::array).collect(toList()));
            break;
        case INT:
            recordBuilder.withArray(entry, (GenericData.Array<Long>) value);
            break;
        case FLOAT:
            recordBuilder.withArray(entry, (GenericData.Array<Double>) value);
            break;
        case BOOLEAN:
            recordBuilder.withArray(entry, (GenericData.Array<Boolean>) value);
            break;
        case LONG:
            recordBuilder.withArray(entry, (GenericData.Array<Long>) value);
            break;
        default:
            throw new IllegalStateException(String.format(ERROR_UNDEFINED_TYPE, entry.getType().name()));
        }
    }

    protected void buildField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder, Entry entry) {
        String logicalType = field.schema().getProp(AVRO_LOGICAL_TYPE);
        switch (field.schema().getType()) {
        case RECORD:
            recordBuilder.withRecord(entry, (Record) value);
            break;
        case ARRAY:
            break;
        case STRING:
            recordBuilder.withString(entry, value != null ? (String) value : null);
            break;
        case BYTES:
            recordBuilder.withBytes(entry, ((java.nio.ByteBuffer) value).array());
            break;
        case INT:
            int ivalue = value != null ? (Integer) value : 0;
            if (AVRO_LOGICAL_TYPE_DATE.equals(logicalType) || AVRO_LOGICAL_TYPE_TIME_MILLIS.equals(logicalType)) {
                recordBuilder.withDateTime(entry, ZonedDateTime.ofInstant(Instant.ofEpochSecond(ivalue), ZoneOffset.UTC));
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
}
