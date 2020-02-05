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
package org.talend.components.common.stream.api.output.impl;

import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;

import org.talend.components.common.stream.api.output.FormatWriter;
import org.talend.components.common.stream.api.output.RecordByteWriter;
import org.talend.components.common.stream.api.output.RecordConverter;

/**
 * Help to write record to an output.
 */
public class RecordWriterChannel<T> extends RecordByteWriter<T> {

    public RecordWriterChannel(RecordConverter<byte[], byte[]> converter, WritableByteChannel out, FormatWriter<T> format,
            T config) {
        super(converter, format, config, () -> Channels.newOutputStream(out));
    }

}
