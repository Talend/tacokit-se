package org.talend.components.common.stream.output.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.OutputFile;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public class TCKParquetWriterBuilder extends ParquetWriter.Builder<Record, TCKParquetWriterBuilder> {

    private Schema schema;

    public TCKParquetWriterBuilder(Path file) {
        super(file);
    }

    public TCKParquetWriterBuilder(OutputFile file) {
        super(file);
    }

    public TCKParquetWriterBuilder withSchema(Schema schema) {
        this.schema = schema;
        return this.self();
    }

    @Override
    protected TCKParquetWriterBuilder self() {
        return this;
    }

    @Override
    protected WriteSupport<Record> getWriteSupport(Configuration conf) {
        return new TCKWriteSupport(this.schema);
    }

}
