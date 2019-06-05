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

package org.talend.components.azure.runtime.converters;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import static java.util.stream.Collectors.toList;
import static org.talend.sdk.component.api.record.Schema.Type.ARRAY;

public class AvroConverter implements RecordConverter<GenericRecord> {

    private RecordBuilderFactory recordBuilderFactory;

    protected Schema schema;

    protected org.apache.avro.Schema avroSchema;

    public static AvroConverter of(RecordBuilderFactory recordBuilderFactory) {
        return new AvroConverter(recordBuilderFactory);
    }

    protected AvroConverter(RecordBuilderFactory recordBuilderFactory) {
        this.recordBuilderFactory = recordBuilderFactory;
    }

    @Override
    public Schema inferSchema(GenericRecord record) {
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        record.getSchema().getFields().stream().map(this::inferAvroField).forEach(builder::withEntry);
        return builder.build();
    }

    @Override
    public Record toRecord(GenericRecord record) {
        if (schema == null) {
            schema = inferSchema(record);
        }
        return avroToRecord(record, record.getSchema().getFields(), recordBuilderFactory.newRecordBuilder(schema));
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
                Schema.Entry e = getSchemaForEntry(name, fromRecord.getSchema());
                if (e != null) {
                    toRecord.put(name, fromRecord.getArray(getJavaClassForType(e.getElementSchema().getType()), name));
                }
                break;
            case STRING:
                toRecord.put(name, fromRecord.getString(name));
                break;
            case BYTES:
                ByteBuffer byteBuffer = ByteBuffer.wrap(fromRecord.getBytes(name));
                toRecord.put(name, byteBuffer);
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

    protected Class<? extends Object> getJavaClassForType(Schema.Type type) {
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

    protected Schema.Entry getSchemaForEntry(String name, Schema schema) {
        for (Schema.Entry e : schema.getEntries()) {
            if (name.equals(e.getName())) {
                return e;
            }
        }
        return null;
    }

    protected org.apache.avro.Schema.Type translateToAvroType(Schema.Type type) {
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
        throw new IllegalStateException(type.toString());
    }

    public org.apache.avro.Schema inferAvroSchema(Schema schema) {
        List<org.apache.avro.Schema.Field> fields = new ArrayList<>();
        for (Schema.Entry e : schema.getEntries()) {
            String name = e.getName();
            String comment = e.getComment();
            Object defaultValue = e.getDefaultValue();
            Schema.Type type = e.getType();
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
                builder.addProp("talend.field.pattern", ""); //
                builder.addProp("java-class", Date.class.getCanonicalName());
                break;
            default:
                throw new IllegalStateException(type.toString());
            }
            org.apache.avro.Schema.Field field = new org.apache.avro.Schema.Field(name, builder, comment, defaultValue);
            fields.add(field);
        }

        return org.apache.avro.Schema.createRecord("talend", "", "org.talend.components.adlsgen2", false, fields);
    }

    protected Record avroToRecord(GenericRecord genericRecord, List<org.apache.avro.Schema.Field> fields) {
        return avroToRecord(genericRecord, fields, null);
    }

    protected Record avroToRecord(GenericRecord genericRecord, List<org.apache.avro.Schema.Field> fields,
            Record.Builder recordBuilder) {
        if (recordBuilder == null) {
            recordBuilder = recordBuilderFactory.newRecordBuilder();
        }
        for (org.apache.avro.Schema.Field field : fields) {
            Object value = genericRecord.get(field.name());
            if (value == null) {
                continue;
            }
            Schema.Entry entry = inferAvroField(field);
            if (field.schema().getType().equals(ARRAY)) {
                buildArrayField(field, value, recordBuilder, entry);

            } else {
                buildField(field, value, recordBuilder, entry);
            }
        }
        return recordBuilder.build();
    }

    protected Schema.Entry inferAvroField(org.apache.avro.Schema.Field field) {
        Schema.Entry.Builder builder = recordBuilderFactory.newEntryBuilder();
        builder.withName(field.name());
        org.apache.avro.Schema.Type type = field.schema().getType();
        String logicalType = field.schema().getProp("logicalType"); // FIXME field.schema().getLogicalType() returns null

        switch (type) {
        case RECORD:
            builder.withType(Schema.Type.RECORD);
            builder.withElementSchema(buildRecordFieldSchema(field));
            break;
        case ENUM:
        case ARRAY:
            builder.withType(Schema.Type.ARRAY);
            builder.withElementSchema(buildArrayFieldSchema(field));
            break;
        case LONG:
            if ("timestamp-millis".equals(logicalType)) {
                builder.withType(Schema.Type.DATETIME);
                break;
            }
        case STRING:
        case BYTES:
        case INT:
            if ("date".equals(logicalType) || "time-milis".equals(logicalType)) {
                builder.withType(Schema.Type.DATETIME);
                break;
            }
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
        Schema.Builder builder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        field.schema().getFields().stream().map(this::inferAvroField).forEach(builder::withEntry);
        return builder.build();
    }

    protected Schema buildArrayFieldSchema(org.apache.avro.Schema.Field field) {
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD);
        Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        entryBuilder.withName(field.name());
        entryBuilder.withType(translateToRecordType(field.schema().getElementType().getType()));
        schemaBuilder.withEntry(entryBuilder.build());
        return schemaBuilder.build();
    }

    /**
     *
     */
    protected Schema.Type translateToRecordType(org.apache.avro.Schema.Type type) {
        switch (type) {
        case RECORD:
            return Schema.Type.RECORD;
        case ARRAY:
            return ARRAY;
        case STRING:
            return Schema.Type.STRING;
        case BYTES:
            return Schema.Type.BYTES;
        case INT:
            return Schema.Type.INT;
        case LONG:
            return Schema.Type.LONG;
        case FLOAT:
            return Schema.Type.FLOAT;
        case DOUBLE:
            return Schema.Type.DOUBLE;
        case BOOLEAN:
            return Schema.Type.BOOLEAN;
        default:
            throw new IllegalArgumentException(); // shouldn't be here
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected void buildArrayField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder,
            Schema.Entry entry) {
        switch (field.schema().getElementType().getType()) {
        case RECORD:
            recordBuilder.withArray(entry, ((GenericData.Array<GenericRecord>) value).stream()
                    .map(record -> avroToRecord(record, field.schema().getFields())).collect(toList()));
            break;
        case STRING:
            recordBuilder.withArray(entry, (ArrayList<String>) value);
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
            throw new IllegalStateException(field.schema().getType().toString());
        }
    }

    protected void buildField(org.apache.avro.Schema.Field field, Object value, Record.Builder recordBuilder,
            Schema.Entry entry) {
        String logicalType = field.schema().getProp("logicalType"); // FIXME field.schema().getLogicalType() returns null
        switch (field.schema().getType()) {
        case RECORD:
            recordBuilder.withRecord(entry, avroToRecord(GenericData.Record.class.cast(value), field.schema().getFields()));
            break;
        case STRING:
            recordBuilder.withString(entry, value.toString());
            break;
        case BYTES:
            recordBuilder.withBytes(entry, ((java.nio.ByteBuffer) value).array());
            break;
        case INT:
            if ("time-millis".equals(logicalType) || "date".equals(logicalType)) {
                recordBuilder.withDateTime(entry,
                        ZonedDateTime.ofInstant(Instant.ofEpochSecond((Integer) value), ZoneId.systemDefault()));
            } else {
                recordBuilder.withInt(entry, (Integer) value);
            }
            break;
        case FLOAT:
            recordBuilder.withFloat(entry, (Float) value);
            break;
        case DOUBLE:
            recordBuilder.withDouble(entry, (Double) value);
            break;
        case BOOLEAN:
            recordBuilder.withBoolean(entry, (Boolean) value);
            break;
        case LONG:
            if ("timestamp-millis".equals(logicalType)) {
                recordBuilder.withDateTime(entry,
                        ZonedDateTime.ofInstant(Instant.ofEpochMilli((Long) value), ZoneId.systemDefault()));
            } else {
                recordBuilder.withLong(entry, (Long) value);
            }
            break;
        case NULL:
            break;
        default:
            throw new IllegalArgumentException(field.schema().getType().toString());
        }
    }
}
