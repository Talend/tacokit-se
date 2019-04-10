/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.adlsgen2.common.format.parquet;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;

import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.configuration.Configuration;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParquetIterator implements Iterator<Record> {

    private final ParquetConverter converter;

    private ParquetReader<GenericRecord> reader;

    private GenericRecord current;

    private ParquetIterator(InputStream inputStream) {
        converter = ParquetConverter.of();
        try {
            File tmp = File.createTempFile("talend-adls-gen2-tmp", ".parquet");
            tmp.deleteOnExit();
            Files.copy(inputStream, tmp.toPath(), StandardCopyOption.REPLACE_EXISTING);
            IOUtils.closeQuietly(inputStream);
            HadoopInputFile hdpIn = HadoopInputFile.fromPath(new Path(tmp.getPath()), new org.apache.hadoop.conf.Configuration());
            reader = AvroParquetReader.<GenericRecord> builder(hdpIn).build();
        } catch (IOException e) {
            log.error("[ParquetIterator] {}", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public boolean hasNext() {
        try {
            current = reader.read();
            if (current == null) {
                reader.close();
                return false;
            }
            return true;
        } catch (IOException e) {
            log.error("[hasNext] {}", e);
            throw new IllegalStateException(e);
        }
    }

    @Override
    public Record next() {
        if ((current != null) || hasNext()) {
            Record r = converter.toRecord(current);
            current = null;
            return r;
        } else {
            return null;
        }
    }

    /**
     *
     */
    public static class Builder {

        private Builder() {
        }

        public static ParquetIterator.Builder of() {
            return new ParquetIterator.Builder();
        }

        public ParquetIterator.Builder withConfiguration(
                @Configuration("parquetConfiguration") final ParquetConfiguration configuration) {
            return this;
        }

        public ParquetIterator parse(InputStream in) {
            return new ParquetIterator(in);
        }

        public ParquetIterator parse(String content) {
            throw new UnsupportedOperationException("#parse(String content)");
        }
    }
}
