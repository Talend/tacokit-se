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
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class ParquetRecordReaderTest {

    @ParameterizedTest
    @ValueSource(strings = { "./sample.parquet", "./testParquet6Records.parquet" })
    void read(final String source) throws IOException {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final ParquetRecordReader reader = new ParquetRecordReader(factory);

        final URL resource = Thread.currentThread().getContextClassLoader().getResource(source);
        final Path path = new Path(resource.getPath());
        final HadoopInputFile inputFile = HadoopInputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
        final Iterator<Record> recordIterator = reader.read(inputFile);
        while (recordIterator.hasNext()) {
            final Record record = recordIterator.next();
            Assertions.assertNotNull(record);
        }

    }
}