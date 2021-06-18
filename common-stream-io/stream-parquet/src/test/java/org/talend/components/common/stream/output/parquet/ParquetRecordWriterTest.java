package org.talend.components.common.stream.output.parquet;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ParquetRecordWriterTest {

    @Test
    void write() throws IOException {
        final URL out = Thread.currentThread().getContextClassLoader().getResource("out");
        File fileOut = new File(out.getPath(), "fic1.parquet");
        if (fileOut.exists()) {
            fileOut.delete();
        }
        Path path = new Path(fileOut.getPath());
        final TCKParquetWriterBuilder builder = new TCKParquetWriterBuilder(path);


        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Schema.Entry f1 = factory.newEntryBuilder().withName("f1").withType(Schema.Type.STRING).build();
        final Schema.Entry f2 = factory.newEntryBuilder().withName("f2").withType(Schema.Type.INT).build();

        final Schema schema = factory.newSchemaBuilder(Type.RECORD).withEntry(f1).withEntry(f2).build();
        final Record record1 = factory.newRecordBuilder(schema).withString(f1, "value1").withInt(f2, 11).build();
        final Record record2 = factory.newRecordBuilder(schema).withString(f1, "value2").withInt(f2, 21).build();

        final ParquetWriter<Record> writer = builder.withSchema(schema).build();
        writer.write(record1);
        writer.write(record2);
        writer.close();
    }
}