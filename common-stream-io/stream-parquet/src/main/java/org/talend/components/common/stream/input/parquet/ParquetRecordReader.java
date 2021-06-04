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
import java.util.Iterator;

import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class ParquetRecordReader {

    private final RecordBuilderFactory factory;

    public Iterator<Record> read(final InputFile input) throws IOException {
        final MessageType messageType = this.findMessageType(input);
        final ReadSupport<Record> readSupport = new TCKReadSupport(factory, messageType);

        final ParquetReader<Record> parquetReader = TCKParquetReader.builder(input, readSupport).build();
        return new ParquetIterator<>(parquetReader);
    }

    private MessageType findMessageType(final InputFile input) throws IOException {
        try (final ParquetFileReader fileReader = ParquetFileReader.open(input)) {
            return fileReader.getFileMetaData().getSchema();
        }
    }
}
