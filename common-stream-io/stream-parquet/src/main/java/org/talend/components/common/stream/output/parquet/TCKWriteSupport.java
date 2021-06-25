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
package org.talend.components.common.stream.output.parquet;

import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.MessageType;
import org.talend.components.common.stream.format.parquet.Constants;
import org.talend.components.common.stream.format.parquet.Name;
import org.talend.components.common.stream.output.parquet.converter.SchemaWriter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class TCKWriteSupport extends WriteSupport<Record> {

    private final Schema recordSchema;

    private RecordConsumer recordConsumer;

    public TCKWriteSupport(final Schema recordSchema) {
        this.recordSchema = recordSchema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        final MessageType messageType = this.extractParquetType();
        // this.parquetType = messageType;
        return new WriteContext(messageType, Collections.emptyMap());
    }

    @Override
    public void prepareForWrite(RecordConsumer recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    @Override
    public void write(final Record record) {
        this.recordConsumer.startMessage();

        this.writeEntries(record);

        this.recordConsumer.endMessage();
    }

    private void writeRecord(final Schema.Entry entry, final int index, final Record record) {
        final Name name = new Name(entry.getName(), entry.getRawName());
        this.recordConsumer.startField(name.parquetName(), index);
        this.recordConsumer.startGroup();

        writeEntries(record);

        this.recordConsumer.endGroup();
        this.recordConsumer.endField(name.parquetName(), index);
    }

    private void writeEntries(final Record record) {

        final List<Schema.Entry> entries = record.getSchema().getEntries();
        IndexValue.from(entries).forEach((IndexValue<Schema.Entry> e) -> {

            final Object value;
            if (e.getValue().getType() == Schema.Type.DATETIME) {
                value = record.getDateTime(e.getValue().getName());
            } else {
                value = record.get(Object.class, e.getValue().getName());
            }
            if (value != null) {
                this.writeField(e.getValue(), e.getIndex(), value);
            }
        });
    }

    private void writeField(final Schema.Entry entry, final int index, final Object value) {
        if (value instanceof Record && entry.getType() == Schema.Type.RECORD) {
            this.writeRecord(entry, index, (Record) value);
        } else if (value instanceof Collection && entry.getType() == Schema.Type.ARRAY) {
            final Name name = new Name(entry.getName(), entry.getRawName());
            final Schema innerSchema = entry.getElementSchema();
            this.writeArray(innerSchema, name, index, (Collection<?>) value);
        } else {
            final Name name = new Name(entry.getName(), entry.getRawName());
            this.recordConsumer.startField(name.parquetName(), index);
            this.writePrimitive(entry.getType(), value);
            this.recordConsumer.endField(name.parquetName(), index);
        }
    }

    private void writePrimitive(final Schema.Type tckType, final Object value) {

        if (tckType == Schema.Type.INT && value instanceof Integer) {
            this.recordConsumer.addInteger((Integer) value);
        } else if (tckType == Schema.Type.LONG && value instanceof Long) {
            this.recordConsumer.addLong((Long) value);
        } else if (tckType == Schema.Type.BOOLEAN && value instanceof Boolean) {
            this.recordConsumer.addBoolean((Boolean) value);
        } else if (tckType == Schema.Type.STRING && value instanceof String) {
            this.recordConsumer.addBinary(Binary.fromCharSequence((String) value));
        } else if (tckType == Schema.Type.FLOAT && value instanceof Float) {
            this.recordConsumer.addFloat((Float) value);
        } else if (tckType == Schema.Type.DOUBLE && value instanceof Double) {
            this.recordConsumer.addDouble((Double) value);
        } else if (tckType == Schema.Type.BYTES && value instanceof byte[]) {
            final Binary binary = Binary.fromConstantByteArray((byte[]) value);
            this.recordConsumer.addBinary(binary);
        } else if (tckType == Schema.Type.DATETIME && value instanceof ZonedDateTime) {
            this.recordConsumer.addLong(((ZonedDateTime) value).toInstant().toEpochMilli());
        }
    }

    /**
     * Write an array in avro.
     * In parquet, write a collection inside list -> element field allow to have array of array.
     * 
     * @param innerSchema : tacokit schema.
     * @param name : name of field.
     * @param index : parquet index.
     * @param values : value to add.
     */
    private void writeArray(final Schema innerSchema, final Name name, final int index, final Collection<?> values) {
        if (values == null || values.isEmpty()) {
            return;
        }

        if (name != null) {
            this.recordConsumer.startField(name.parquetName(), index);
        }

        this.recordConsumer.startGroup();
        this.recordConsumer.startField(Constants.LIST_NAME, 0);
        this.recordConsumer.startGroup();
        this.recordConsumer.startField(Constants.ELEMENT_NAME, 0);
        if (innerSchema.getType() == Schema.Type.RECORD) {
            values.stream().filter(Record.class::isInstance).map(Record.class::cast).forEach((Record rec) -> {
                TCKWriteSupport.this.recordConsumer.startGroup();
                TCKWriteSupport.this.writeEntries(rec);
                TCKWriteSupport.this.recordConsumer.endGroup();
            });
        } else if (innerSchema.getType() == Schema.Type.ARRAY) {
            final Schema elementSchema = innerSchema.getElementSchema();
            values.forEach((Object e) -> this.writeArray(elementSchema, null, index, (Collection<?>) e));
        } else {
            values.forEach((Object value) -> this.writePrimitive(innerSchema.getType(), value));
        }
        this.recordConsumer.endField(Constants.ELEMENT_NAME, 0);
        this.recordConsumer.endGroup();
        this.recordConsumer.endField(Constants.LIST_NAME, 0);
        this.recordConsumer.endGroup();
        if (name != null) {
            this.recordConsumer.endField(name.parquetName(), index);
        }
    }

    private MessageType extractParquetType() {
        final SchemaWriter converter = new SchemaWriter();
        return converter.convert(this.recordSchema);
    }

}
