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
package org.talend.components.common.stream.output.json;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.json.Json;
import javax.json.JsonObject;
import javax.json.JsonStructure;
import javax.json.JsonValue.ValueType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.common.stream.api.output.RecordWriter;
import org.talend.components.common.stream.format.json.JsonConfiguration;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class JsonWriterSupplierTest {

    @Test
    public void write() throws IOException {

        final JsonWriterSupplier supplier = new JsonWriterSupplier();
        final JsonConfiguration configuration = new JsonConfiguration();

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        final RecordWriter writer = supplier.getWriter(() -> out, configuration);
        writer.init(configuration);

        RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        Record rec = factory.newRecordBuilder().withString("hi", "v1").withInt("val", 1).build();

        writer.add(rec);
        writer.add(rec);
        writer.flush();

        writer.end();

        final JsonStructure json = Json.createReader(new ByteArrayInputStream(out.toByteArray())).read();
        Assertions.assertNotNull(json);
        Assertions.assertEquals(ValueType.ARRAY, json.getValueType());
        JsonObject obj = json.asJsonArray().getJsonObject(0);
        Assertions.assertNotNull(obj);
        Assertions.assertEquals("v1", obj.getString("hi"));
        Assertions.assertEquals(1, obj.getInt("val"));
    }

}