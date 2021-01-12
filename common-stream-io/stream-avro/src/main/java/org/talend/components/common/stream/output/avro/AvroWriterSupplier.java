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
package org.talend.components.common.stream.output.avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.talend.components.common.stream.api.output.RecordConverter;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.output.TargetFinder;
import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.avro.AvroConfiguration;

public class AvroWriterSupplier implements RecordWriterSupplier {

    @Override
    public RecordWriter getWriter(final TargetFinder target, final ContentFormat config) {
        if (!AvroConfiguration.class.isInstance(config)) {
            throw new IllegalArgumentException("Try to get avro-writer with other than avro config.");
        }

        AvroConfiguration avroConfig = (AvroConfiguration) config;

        final RecordConverter<GenericRecord, Schema> converter = new RecordToAvro("records");
        final AvroOutput output = AvroOutput.buildOutput(avroConfig, target);
        return new AvroRecordWriter(converter, output);
    }
}
