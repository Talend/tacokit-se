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
import org.apache.avro.*;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
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
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Locale;

@Slf4j
public class AvroMessageConverterTest {

    private AvroMessageConverter beanUnderTest;

    private PubSubDataSet dataSet;

    @BeforeEach
    public void init() {
        beanUnderTest = new AvroMessageConverter();
        beanUnderTest.setI18nMessage(new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader()));
        beanUnderTest.setRecordBuilderFactory(new RecordBuilderFactoryImpl(null));

        dataSet = new PubSubDataSet();
    }

    private Schema getAvroSchema() {
        return SchemaBuilder.record("testRecord").namespace("org.talend.test").fields().name("aInt").type().intType().noDefault()
                .name("aString").type().stringType().noDefault().name("aBoolean").type().booleanType().noDefault()
                .name("someBytes").type().bytesType().noDefault().name("aDouble").type().doubleType().noDefault().name("aFloat")
                .type().floatType().noDefault().name("aLong").type().longType().noDefault().name("innerArray")
                .type(SchemaBuilder.array().items(SchemaBuilder.builder().stringType())).withDefault(null).name("innerRecord")
                .type(getInnerTypeAvroSchema()).withDefault(null).endRecord();
    }

    private Schema getInnerTypeAvroSchema() {
        return SchemaBuilder.record("innerRecord").namespace("org.talend.test").fields().requiredString("field0")
                .requiredString("field1").endRecord();
    }

    private GenericRecord getAvroRecord() {
        return new GenericRecordBuilder(getAvroSchema()).set("aInt", 42).set("aString", "Talend").set("aBoolean", true)
                .set("someBytes", ByteBuffer.wrap(new byte[] { 0x00, 0x01, 0x02 })).set("aDouble", Math.PI)
                .set("aFloat", 3.1415926535f).set("aLong", 123456789l)
                .set("innerArray", Arrays.asList(new String[] { "a", "b", "c" }))
                .set("innerRecord", new GenericRecordBuilder(getInnerTypeAvroSchema()).set("field0", "innerField0")
                        .set("field1", "innerField1").build())
                .build();
    }

    @Test
    public void testFormats() {
        Arrays.stream(PubSubDataSet.ValueFormat.values()).forEach(this::testFormat);
    }

    private void testFormat(PubSubDataSet.ValueFormat format) {
        dataSet.setValueFormat(format);
        dataSet.setAvroSchema(getAvroSchema().toString(true));
        beanUnderTest.init(dataSet);
        Assertions.assertEquals(format == PubSubDataSet.ValueFormat.AVRO, beanUnderTest.acceptFormat(format),
                "AvroMessageConverter must accept only Avro");
    }

    @Test
    public void convertTest() throws IOException {

        dataSet.setValueFormat(PubSubDataSet.ValueFormat.AVRO);
        dataSet.setAvroSchema(getAvroSchema().toString(false));
        beanUnderTest.init(dataSet);

        GenericRecord genericRecord = getAvroRecord();

        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(getAvroSchema());
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        BinaryEncoder out = EncoderFactory.get().binaryEncoder(bout, null);
        writer.write(genericRecord, out);
        out.flush();

        PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFrom(bout.toByteArray())).build();

        Record record = beanUnderTest.convertMessage(message);

        Assertions.assertNotNull(record, "Record should not be null");
        Assertions.assertNotNull(record.getSchema(), "Record schema should not be null");
        Assertions.assertNotNull(record.getArray(String.class, "innerArray"), "Inner array is null");

    }
}
