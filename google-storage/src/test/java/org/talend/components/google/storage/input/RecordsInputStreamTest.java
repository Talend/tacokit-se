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
package org.talend.components.google.storage.input;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.RecordIORepository;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.api.input.RecordReaderSupplier;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator.Type;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents(value = "org.talend.components.google.storage")
class RecordsInputStreamTest {

    @Service
    private RecordIORepository repository;

    @Service
    private RecordBuilderFactory factory;

    @Test
    void records() {
        final CSVConfiguration csvConfig = new CSVConfiguration();
        csvConfig.getFieldSeparator().setFieldSeparatorType(Type.COMMA);
        csvConfig.setQuotedValue('"');
        csvConfig.setEscape('\\');

        final RecordReaderSupplier recordReaderSupplier = this.repository.findReader(CSVConfiguration.class);
        final RecordReader reader = recordReaderSupplier.getReader(this.factory, csvConfig);

        final AtomicInteger countClosed = new AtomicInteger(0);
        final ByteArrayInputStream input = new ByteArrayInputStream("line1\nline2".getBytes()) {

            @Override
            public void close() throws IOException {
                countClosed.getAndIncrement();
                super.close();
            }
        };
        final RecordsInputStream recordsGetter = new RecordsInputStream(reader, () -> input);
        final Iterator<Record> records = recordsGetter.records();
        Assertions.assertEquals(0, countClosed.get());

        final AtomicInteger countRecord = new AtomicInteger(0);
        records.forEachRemaining((Record r) -> countRecord.getAndIncrement());
        Assertions.assertEquals(1, countClosed.get());
        Assertions.assertEquals(2, countRecord.get());

        final RecordsInputStream recordsNull = new RecordsInputStream(reader, null);
        Assertions.assertFalse(recordsNull.records().hasNext());

        final RecordsInputStream recordsNull2 = new RecordsInputStream(reader, () -> null);
        Assertions.assertFalse(recordsNull2.records().hasNext());
    }

}