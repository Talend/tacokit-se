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
package org.talend.components.common.stream.output.csv;

import java.util.Arrays;

import org.apache.commons.csv.CSVFormat;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class CSVRecordConverterTest {

    @Test
    void fromRecord() {
        final CSVFormat format = CSVFormat.newFormat(';').withQuote('"').withEscape('\\');
        final CSVRecordConverter converter = new CSVRecordConverter(format);

        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final Record record1 = factory.newRecordBuilder().withString("field1", "value1")
                .withRecord("sub", factory.newRecordBuilder().withString("innerField", "innerValue").build())
                .withArray(
                        factory.newEntryBuilder().withType(Type.ARRAY).withName("subArray")
                                .withElementSchema(factory.newSchemaBuilder(Type.STRING).build()).build(),
                        Arrays.asList("v1", "v2"))
                .withInt("field2", 4).build();
        final byte[] csvBytes = converter.fromRecord(record1);
        final String csvLine = new String(csvBytes);

        Assertions.assertEquals("value1;innerValue;4", csvLine);

        final byte[] schemaCSV = converter.fromRecordSchema(record1.getSchema());
        final String csvSchema = new String(schemaCSV);
        Assertions.assertEquals("field1;sub.innerField;field2", csvSchema);
    }
}