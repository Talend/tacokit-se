package org.talend.components.common.stream.input.parquet;

import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ParquetRecordReaderTest {

    @Test
    void read() throws IOException {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final ParquetRecordReader reader = new ParquetRecordReader(factory);

        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./sample.parquet");
        final Path path = new Path(resource.getPath());
        final HadoopInputFile inputFile = HadoopInputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
        final Iterator<Record> recordIterator = reader.read(inputFile);
        while (recordIterator.hasNext()) {
            final Record record = recordIterator.next();
            Assertions.assertNotNull(record);
        }

    }
}