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
package org.talend.components.adlsgen2.common.format.unknown;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.adlsgen2.AdlsGen2TestBase;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.junit5.WithComponents;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@WithComponents("org.talend.components.adlsgen2")
class UnknownConverterTest extends AdlsGen2TestBase {

    private Schema schema;

    private UnknownConverter converter;

    private String content = "Another content with no meaning...";

    @BeforeEach
    protected void setUp() throws Exception {
        super.setUp();
        converter = UnknownConverter.of();
        schema = recordBuilderFactory.newSchemaBuilder(Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("content").withType(Type.STRING).build()).build();
    }

    @Test
    void of() {
        assertNotNull(UnknownConverter.of());
    }

    @Test
    void inferSchema() {
        assertEquals(schema, converter.inferSchema(content));
    }

    @Test
    void toRecord() {
        assertEquals(content, converter.toRecord(content).getString("content"));
    }

    @Test
    void fromRecord() {
        Record record = recordBuilderFactory.newRecordBuilder().withString("content", content).build();
        assertEquals(String.format("content:{%s}", content), converter.fromRecord(record));
        record = recordBuilderFactory.newRecordBuilder() //
                .withString("field0", "Bonjour") //
                .withString("field1", "Olà") //
                .withInt("field2", 71) //
                .build();
        assertEquals("field0:{Bonjour},field1:{Olà},field2:{71}", converter.fromRecord(record));
        assertEquals(
                "string1:{Bonjour},string2:{Olà},int:{71},boolean:{true},long:{1971},datetime:{61516620000000},float:{20.5},double:{20.5}",
                converter.fromRecord(versatileRecord));
    }
}
