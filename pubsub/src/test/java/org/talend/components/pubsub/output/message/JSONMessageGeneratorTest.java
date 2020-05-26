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
package org.talend.components.pubsub.output.message;

import com.google.pubsub.v1.PubsubMessage;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.components.pubsub.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.internationalization.InternationalizationServiceFactory;
import org.talend.sdk.component.runtime.manager.service.RecordServiceImpl;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Arrays;
import java.util.Locale;

public class JSONMessageGeneratorTest {

    private JSONMessageGenerator beanUnderTest;

    private PubSubDataSet dataSet;

    @BeforeEach
    public void init() {
        beanUnderTest = new JSONMessageGenerator();
        beanUnderTest.setI18nMessage(new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader()));
        beanUnderTest.setRecordService(new RecordServiceImpl(null, null, null, null, null));

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
                "JSONMessageGenerator must accept only JSON");
    }

    @Test
    public void generateTest() {
        dataSet.setValueFormat(PubSubDataSet.ValueFormat.JSON);
        beanUnderTest.init(dataSet);

        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("name").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("age").withType(Schema.Type.INT).build())
                .withEntry(rbf.newEntryBuilder().withName("state").withType(Schema.Type.STRING).build())
                .withEntry(
                        rbf.newEntryBuilder().withName("nullableString").withType(Schema.Type.STRING).withNullable(true).build())

                .build();
        Record record = rbf.newRecordBuilder(schema).withString("name", "John Smith").withInt("age", 42).withString("state", "CA")
                .build();

        PubsubMessage message = beanUnderTest.generateMessage(record);
        Assertions.assertNotNull(message, "Message is null");
    }
}
