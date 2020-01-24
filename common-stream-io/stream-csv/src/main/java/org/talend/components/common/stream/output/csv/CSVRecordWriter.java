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
package org.talend.components.common.stream.output.csv;

import org.talend.components.common.stream.CSVHelper;
import org.talend.components.common.stream.api.output.RecordByteWriter;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterRepository;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.output.WritableTarget;
import org.talend.components.common.stream.format.CSVConfiguration;
import org.talend.components.common.stream.format.ContentFormat;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CSVRecordWriter extends RecordByteWriter {

    static {
        RecordWriterRepository.getInstance().put(CSVConfiguration.class, new CSVWriterSupplier());
    }

    public CSVRecordWriter(CSVConfiguration config, WritableTarget<byte[]> out) {
        super(new CSVRecordConverter(CSVHelper.getCsvFormat(config)), new CSVFormatWriter(), config, out);
    }

    public static class CSVWriterSupplier implements RecordWriterSupplier<byte[]> {

        @Override
        public RecordWriter getWriter(WritableTarget<byte[]> target, ContentFormat config) {
            assert config instanceof CSVConfiguration : "csv reader not with csv config";
            CSVConfiguration csvConfig = (CSVConfiguration) config;

            return new CSVRecordWriter(csvConfig, target);
        }
    }
}
