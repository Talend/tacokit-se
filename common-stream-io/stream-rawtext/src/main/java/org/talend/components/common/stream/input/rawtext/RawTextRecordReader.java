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
package org.talend.components.common.stream.input.rawtext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.rawtext.ExtendedRawTextConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class RawTextRecordReader implements RecordReader {

    private final RecordBuilderFactory recordFactory;

    private final ExtendedRawTextConfiguration extendedConfiguration;

    public RawTextRecordReader(final RecordBuilderFactory recordFactory,
            final ExtendedRawTextConfiguration extendedConfiguration) {
        this.recordFactory = recordFactory;
        this.extendedConfiguration = extendedConfiguration;
    }

    @Override
    public Iterator<Record> read(InputStream reader) {
        if (reader == null) {
            if (this.extendedConfiguration.isForceOneRow()) {
                return Collections.singletonList((Record) null).iterator();
            } else {
                List<Record> l = Collections.emptyList();
                return l.iterator();
            }
        }

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            final byte[] buffer = new byte[512];
            int nbe = reader.read(buffer);
            while (nbe > 0) {
                out.write(buffer, 0, nbe);
                nbe = reader.read(buffer);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Can't build raw text record.", e);
        }

        try {
            Record record = recordFactory.newRecordBuilder()
                    .withString("content", out.toString(this.extendedConfiguration.getCharset())).build();
            return Collections.singletonList(record).iterator();
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(
                    "Unknown encoding for RawTextRecordReader '" + this.extendedConfiguration.getCharset() + "'", e);
        }

    }

    @Override
    public void close() {
        // Nothing to do
    }
}
