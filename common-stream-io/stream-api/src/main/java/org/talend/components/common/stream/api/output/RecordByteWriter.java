/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.stream.api.output;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;

import org.talend.sdk.component.api.record.Record;

public class RecordByteWriter<T> implements RecordWriter {

    /** to serialize record to an array of byte. */
    protected final RecordConverter<byte[], byte[]> converter;

    protected final FormatWriter<T> format;

    private final T config;

    private final TargetFinder target;

    private final OutputStream out;

    protected boolean first = true;

    public RecordByteWriter(RecordConverter<byte[], byte[]> converter, FormatWriter<T> format, T config, TargetFinder target) {
        this.converter = converter;
        this.format = format;
        this.config = config;
        this.target = target;

        try {
            this.out = this.target.find();
        } catch (IOException ex) {
            throw new UncheckedIOException("", ex);
        }
    }

    @Override
    public void end() throws IOException {
        this.write(this.format.end(this.config));
        this.close();
    }

    @Override
    public void add(Record record) throws IOException {
        this.write(this.firstOrBetween(record));
        final byte[] recordContent = this.converter.fromRecord(record);
        this.write(recordContent);
    }

    protected void write(byte[] data) throws IOException {
        if (data != null && data.length > 0) {
            this.out.write(data);
        }
    }

    @Override
    public void flush() throws IOException {
        this.out.flush();
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    protected byte[] firstOrBetween(Record record) {
        byte[] result = null;
        if (first && this.format != null) {
            this.first = false;
            result = this.format.start(this.config, record);
        } else if (format != null) {
            result = this.format.between(this.config);
        }

        return result;
    }
}
