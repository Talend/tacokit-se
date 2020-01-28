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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.api.output.RecordWriterRepository;
import org.talend.components.common.stream.api.output.RecordWriterSupplier;
import org.talend.components.common.stream.api.output.impl.OutputStreamTarget;
import org.talend.components.common.stream.format.CSVConfiguration;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class CSVRecordWriterTest {

    @Test
    public void writeCSV() throws IOException {
        CSVWriterSupplier supplier = new CSVWriterSupplier();

        final RecordWriterSupplier<byte[]> recordWriterSupplier = (RecordWriterSupplier<byte[]>) RecordWriterRepository
                .getInstance().get(CSVConfiguration.class);

        final CSVConfiguration config = new CSVConfiguration();
        config.setLineConfiguration(new LineConfiguration());
        config.getLineConfiguration().setLineSeparator("\n");
        config.setEscape('\\');
        config.setFieldSeparator(';');
        config.setQuotedValue('"');

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final OutputStreamTarget target = new OutputStreamTarget(out);
        final RecordWriter writer = recordWriterSupplier.getWriter(target, config);

        writer.init(config);

        final List<Record> records = buildRecords();
        writer.add(records);

        writer.flush();
        writer.end();

        Assertions.assertEquals("mike;45\nbob;11\n\"ice;peak\";13", out.toString());
    }

    private List<Record> buildRecords() {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final Record record1 = factory.newRecordBuilder().withString("hello", "mike").withInt("xx", 45).build();

        final Record record2 = factory.newRecordBuilder().withString("hello", "bob").withInt("xx", 11).build();

        final Record record3 = factory.newRecordBuilder().withString("hello", "ice;peak").withInt("xx", 13).build();

        return Arrays.asList(record1, record2, record3);
    }

}