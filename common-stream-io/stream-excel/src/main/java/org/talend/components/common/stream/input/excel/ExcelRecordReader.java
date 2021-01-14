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
package org.talend.components.common.stream.input.excel;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Iterator;

import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExcelRecordReader implements RecordReader {

    /** excel configuration */
    private final ExcelConfiguration configuration;

    /** converter from excel row to talend record. */
    private final RecordBuilderFactory factory;

    public ExcelRecordReader(ExcelConfiguration configuration, RecordBuilderFactory factory) {
        this.configuration = configuration;
        this.factory = factory;
    }

    @Override
    public Iterator<Record> read(InputStream in) {
        try {
            final FormatReader reader = FormatReader.findReader(this.configuration.getExcelFormat(), this.factory);
            return reader.read(in, this.configuration);
        } catch (IOException exIO) {
            log.error("Error while reading excel input", exIO);
            throw new UncheckedIOException("Error while reading excel input", exIO);
        }
    }

    @Override
    public void close() {
    }
}
