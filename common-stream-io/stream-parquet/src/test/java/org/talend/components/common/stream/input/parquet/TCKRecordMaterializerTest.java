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
package org.talend.components.common.stream.input.parquet;

import java.io.IOException;
import java.net.URL;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class TCKRecordMaterializerTest {

    @Test
    void getCurrentRecord() throws IOException {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./sample.parquet");

        final Path path = new Path(resource.getPath());
        final MessageType messageType = this.findMessageType(path);

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final ReadSupport<Record> readSupport = new TCKReadSupport(factory, messageType);

        final ParquetReader.Builder<Record> recordBuilder = ParquetReader.builder(readSupport, path);
        try (final ParquetReader<Record> parquetReader = recordBuilder.build()) {
            final Record record = parquetReader.read();
            Assertions.assertNotNull(record);

            final Record record1 = parquetReader.read();
            Assertions.assertNotNull(record1);

            final Record record2 = parquetReader.read();
            Assertions.assertNotNull(record2);

            final Record record3 = parquetReader.read();
            Assertions.assertNull(record3);
        }
    }

    private MessageType findMessageType(final Path path) throws IOException {
        final HadoopInputFile hdpIn = HadoopInputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
        try (final ParquetFileReader fileReader = ParquetFileReader.open(hdpIn)) {
            return fileReader.getFileMetaData().getSchema();
        }
    }
}