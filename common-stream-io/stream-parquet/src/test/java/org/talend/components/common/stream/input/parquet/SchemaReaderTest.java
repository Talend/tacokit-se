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
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.input.parquet.converter.SchemaReader;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class SchemaReaderTest {

    @Test
    void convert() throws IOException {
        final URL resource = Thread.currentThread().getContextClassLoader().getResource("./sample.parquet");

        HadoopInputFile hdpIn = HadoopInputFile.fromPath(new Path(resource.getPath()),
                new org.apache.hadoop.conf.Configuration());
        ParquetFileReader fileReader = new ParquetFileReader(hdpIn, ParquetReadOptions.builder().build());
        final MessageType messageType = fileReader.getFileMetaData().getSchema();

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        SchemaReader reader = new SchemaReader(factory);

        final Schema schema = reader.convert(messageType);
        Assertions.assertNotNull(schema);

        Assertions.assertEquals(Schema.Type.RECORD, schema.getType());
        final Schema.Entry topics = schema.getEntry("topics");
        Assertions.assertEquals(Schema.Type.RECORD, topics.getType());

        final Schema.Entry array = topics.getElementSchema().getEntry("array");
        final Schema arrayType = array.getElementSchema();
        Assertions.assertNotNull(arrayType);
        Assertions.assertEquals(Schema.Type.STRING, arrayType.getType());
    }
}