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
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.format.Encoding;
import org.talend.components.common.stream.format.excel.ExcelConfiguration;
import org.talend.components.common.stream.format.excel.ExcelConfiguration.ExcelFormat;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class HTMLReaderTest {

    @Test
    void read() throws IOException {
        try (InputStream input = Thread.currentThread().getContextClassLoader().getResourceAsStream("./html/HTML-utf8.html")) {

            final HTMLReader reader = new HTMLReader(new RecordBuilderFactoryImpl("test"));
            final ExcelConfiguration configuration = new ExcelConfiguration();
            configuration.getEncoding().setEncodingType(Encoding.Type.UTF8);
            configuration.setExcelFormat(ExcelFormat.HTML);

            final Iterator<Record> recordIterator = reader.read(input, configuration);
            int nbe = 0;
            while (recordIterator.hasNext()) {
                final Record record = recordIterator.next();
                Assertions.assertNotNull(record);
                final String owner = record.getString("Account_Owner");
                Assertions.assertNotNull(owner);
                nbe++;
            }
            Assertions.assertEquals(16, nbe);
        }
    }
}