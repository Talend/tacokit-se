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
package org.talend.components.pubsub.input.converter;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.runtime.internationalization.InternationalizationServiceFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Locale;

@Slf4j
public class JSonMessageConverterTest {

    private JSonMessageConverter beanUnderTest;

    private PubSubDataSet dataSet;

    @BeforeEach
    public void init() {
        beanUnderTest = new JSonMessageConverter();
        beanUnderTest.setI18nMessage(new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader()));
        beanUnderTest.setRecordBuilderFactory(new RecordBuilderFactoryImpl(null));

        dataSet = new PubSubDataSet();
    }

    @Test
    public void testFormats() {
        Arrays.stream(PubSubDataSet.ValueFormat.values()).forEach(this::testFormat);
    }

    private void testFormat(PubSubDataSet.ValueFormat format) {
        dataSet.setValueFormat(format);
        beanUnderTest.init(dataSet);
        Assertions.assertEquals(format == PubSubDataSet.ValueFormat.JSON, beanUnderTest.acceptFormat(format),
                "JSonMessageConverter must accept only JSON");
    }

    @Test
    public void convertTest() throws IOException {

        dataSet.setValueFormat(PubSubDataSet.ValueFormat.JSON);
        beanUnderTest.init(dataSet);

        ByteArrayOutputStream jsonBytes = new ByteArrayOutputStream();
        try (InputStream jsonStream = Thread.currentThread().getContextClassLoader().getResourceAsStream("json/sample.json")) {
            byte[] buffer = new byte[1024];
            int read = 0;
            while ((read = jsonStream.read(buffer)) > 0) {
                jsonBytes.write(buffer, 0, read);
            }
        }
        jsonBytes.flush();
        jsonBytes.close();

        PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(jsonBytes.toByteArray())).build();

        Record record = beanUnderTest.convertMessage(message);

        Assertions.assertNotNull(record, "Record is null.");
        Assertions.assertNotNull(record.getSchema(), "Record schema is null.");
        Assertions.assertNotNull(record.getArray(String.class, "innerArray"), "innerArray is null");
        record.getArray(String.class, "innerArray").stream().map(Object::getClass)
                .forEach(c -> Assertions.assertEquals(String.class, c, "Array item must be string"));
        Assertions.assertNotNull(record.getRecord("innerRecord"), "inner record is null.");
        Assertions.assertNotNull(record.getRecord("innerRecord").getRecord("field3"), "inner record field3 is null.");
        Assertions.assertNotNull(record.getRecord("innerRecord").getRecord("field3").getArray(Double.class, "field3_1"),
                "inner record field3_1 is null.");

    }
}
