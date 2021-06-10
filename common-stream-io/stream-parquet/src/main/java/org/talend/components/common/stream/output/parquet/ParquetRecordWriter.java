package org.talend.components.common.stream.output.parquet;

import org.apache.parquet.hadoop.api.WriteSupport;
import org.talend.sdk.component.api.record.Record;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ParquetRecordWriter implements AutoCloseable {

    private final WriteSupport<Record> writer;

    public void write(final Record record) {
        this.writer.write(record);
    }

    @Override
    public void close() {
        this.writer.finalizeWrite();
    }
}
