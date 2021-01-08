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
package org.talend.components.common.stream.input.csv;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.input.RecordReader;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.components.common.stream.format.LineConfiguration.LineSeparatorType;
import org.talend.components.common.stream.format.csv.CSVConfiguration;
import org.talend.components.common.stream.format.csv.FieldSeparator;
import org.talend.components.common.stream.format.csv.FieldSeparator.Type;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class CSVReaderSupplierTest {

    @Test
    void testCSVPipeline() throws IOException {
        final CSVConfiguration configuration = new CSVConfiguration();
        configuration.setQuotedValue('"');
        configuration.setEscape('\\');
        configuration.setFieldSeparator(new FieldSeparator());
        configuration.getFieldSeparator().setFieldSeparatorType(Type.COMMA);

        configuration.setLineConfiguration(new LineConfiguration());
        configuration.getLineConfiguration().setLineSeparatorType(LineSeparatorType.OTHER);
        configuration.getLineConfiguration().setLineSeparator("|");

        CSVReaderSupplier supplier = new CSVReaderSupplier();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final RecordReader reader = supplier.getReader(factory, configuration);

        URL urlFic = Thread.currentThread().getContextClassLoader().getResource("./CSV_PipeLineSep.csv");
        try (FileInputStream fic = new FileInputStream(urlFic.getPath())) {
            final Iterator<Record> read = reader.read(fic);

            while (read.hasNext()) {
                final Record record = read.next();
                Assertions.assertNotNull(record);
            }
        }
    }

}