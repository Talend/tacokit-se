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
import org.talend.components.common.stream.output.parquet.converter.SchemaWriter;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

public class TCKWriteSupport extends WriteSupport<Record> {

    private MessageType parquetType = null;

    private final Schema recordSchema;

    private RecordConsumer recordConsumer;

    public TCKWriteSupport(final Schema recordSchema) {
        this.recordSchema = recordSchema;
    }

    @Override
    public WriteContext init(Configuration configuration) {
        final MessageType messageType = this.extractParquetType();
        this.parquetType = messageType;
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

    private void writeRecord(
            final Schema.Entry entry,
            final int index,
            final Record record) {
        this.recordConsumer.startField(entry.getOriginalFieldName(), index);
        this.recordConsumer.startGroup();

        writeEntries(record);

        this.recordConsumer.endGroup();
        this.recordConsumer.endField(entry.getOriginalFieldName(), index);
    }

    private void writeEntries(final Record record) {

        final List<Entry> entries = record.getSchema().getEntries();
        IndexValue.from(entries).forEach(
                (IndexValue<Entry> e) -> {

                    final Object value;
                    if (e.getValue().getType() == Schema.Type.DATETIME) {
                        value = record.getDateTime(e.getValue().getOriginalFieldName());
                    }
                    else {
                        value = record.get(Object.class, e.getValue().getOriginalFieldName());
                    }
                    if (value != null) {
                        this.writeField(e.getValue(), e.getIndex(), value);
                    }
                }
        );
    }

    private void writeField(final Schema.Entry entry,
            final int index,
            final Object value)  {
        if (value instanceof Record && entry.getType() == Schema.Type.RECORD) {
            this.writeRecord(entry, index, (Record) value);
        }
        else if (value instanceof Collection && entry.getType() == Schema.Type.ARRAY) {
            this.writeArray(entry, index, (List<?>) value);
        }
        else {
            this.recordConsumer.startField(entry.getOriginalFieldName(), index);
            this.writePrimitive(entry.getType(), value);
            this.recordConsumer.endField(entry.getOriginalFieldName(), index);
        }
    }

    private void writePrimitive(final Schema.Type tckType, final Object value) {

        if (tckType == Schema.Type.INT && value instanceof Integer) {
            this.recordConsumer.addInteger((Integer) value);
        }
        else if (tckType == Schema.Type.LONG && value instanceof Long) {
            this.recordConsumer.addLong((Long) value);
        }
        else if (tckType == Schema.Type.BOOLEAN && value instanceof Boolean) {
            this.recordConsumer.addBoolean((Boolean) value);
        }
        else if (tckType == Schema.Type.FLOAT && value instanceof Float) {
            this.recordConsumer.addFloat((Float) value);
        }
        else if (tckType == Schema.Type.DOUBLE && value instanceof Double) {
            this.recordConsumer.addDouble((Double) value);
        }
        else if (tckType == Schema.Type.BYTES && value instanceof byte[]) {
            final Binary binary = Binary.fromConstantByteArray((byte[]) value);
            this.recordConsumer.addBinary(binary);
        }
        else if (tckType == Schema.Type.DATETIME && value instanceof ZonedDateTime) {
            this.recordConsumer.addLong(((ZonedDateTime) value).toInstant().toEpochMilli());
        }
    }

    private void writeArray(final Schema.Entry entry,
            final int index,
            final List<?> values) {
        this.recordConsumer.startField(entry.getOriginalFieldName(), index);
        this.recordConsumer.startGroup();

        if (entry.getElementSchema().getType() == Schema.Type.RECORD) {
            values.stream()
                    .filter(Record.class::isInstance)
                    .map(Record.class::cast)
                    .forEach((Record rec) -> {
                        TCKWriteSupport.this.recordConsumer.startGroup();
                        this.writeEntries(rec);
                        TCKWriteSupport.this.recordConsumer.endGroup();
                    });
        }
        else if (entry.getElementSchema().getType() == Schema.Type.ARRAY) {

        }
        else {
            values.forEach((Object value) -> this.writePrimitive(entry.getType(), value));
        }


        this.recordConsumer.endGroup();
        this.recordConsumer.endField(entry.getOriginalFieldName(), index);
    }


    private MessageType extractParquetType() {
        final SchemaWriter converter = new SchemaWriter();
        return converter.convert(this.recordSchema);
    }


}
