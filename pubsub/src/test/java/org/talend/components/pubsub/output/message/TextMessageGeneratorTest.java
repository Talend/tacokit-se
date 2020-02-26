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
import org.talend.sdk.component.runtime.manager.service.RecordPointerFactoryImpl;
import org.talend.sdk.component.runtime.manager.service.RecordServiceImpl;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Arrays;
import java.util.Locale;

public class TextMessageGeneratorTest {

    private TextMessageGenerator beanUnderTest;

    private PubSubDataSet dataSet;

    @BeforeEach
    public void init() {
        beanUnderTest = new TextMessageGenerator();
        beanUnderTest.setI18nMessage(new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader()));
        beanUnderTest.setRecordService(new RecordServiceImpl(null, null));
        beanUnderTest.setRecordPointerFactory(new RecordPointerFactoryImpl(null));

        dataSet = new PubSubDataSet();
    }

    @Test
    public void generateTest() {
        dataSet.setValueFormat(PubSubDataSet.ValueFormat.TEXT);
        dataSet.setPathToText("/content");
        beanUnderTest.init(dataSet);

        String text = "This is a text message";
        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("name").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("age").withType(Schema.Type.INT).build())
                .withEntry(rbf.newEntryBuilder().withName("content").withType(Schema.Type.STRING).build())
                .build();
        Record record = rbf.newRecordBuilder(schema)
                .withString("name", "John Smith")
                .withInt("age", 42)
                .withString("content", text)
                .build();

        PubsubMessage message = beanUnderTest.generateMessage(record);
        Assertions.assertNotNull(message, "Message is null");
        Assertions.assertEquals(text, message.getData().toStringUtf8());
    }
    @Test
    public void generateNoDataTest() {
        dataSet.setValueFormat(PubSubDataSet.ValueFormat.TEXT);
        dataSet.setPathToText("/contentFail");
        beanUnderTest.init(dataSet);

        String text = "This is a text message";
        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("name").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("age").withType(Schema.Type.INT).build())
                .withEntry(rbf.newEntryBuilder().withName("content").withType(Schema.Type.STRING).build())
                .build();
        Record record = rbf.newRecordBuilder(schema)
                .withString("name", "John Smith")
                .withInt("age", 42)
                .withString("content", text)
                .build();

        PubsubMessage message = beanUnderTest.generateMessage(record);
        Assertions.assertNull(message, "Message should be null");
    }


}
