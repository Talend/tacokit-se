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
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.util.Arrays;
import java.util.Locale;

public class CSVMessageGeneratorTest {

    private CSVMessageGenerator beanUnderTest;

    private PubSubDataSet dataSet;

    @BeforeEach
    public void init() {
        beanUnderTest = new CSVMessageGenerator();
        beanUnderTest.setI18nMessage(new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader()));

        dataSet = new PubSubDataSet();
    }

    @Test
    public void testFormats() {
        Arrays.stream(PubSubDataSet.ValueFormat.values()).forEach(this::testFormat);
    }

    private void testFormat(PubSubDataSet.ValueFormat format) {
        dataSet.setValueFormat(format);
        dataSet.setFieldDelimiter(PubSubDataSet.CSVDelimiter.SEMICOLON);
        beanUnderTest.init(dataSet);
        Assertions.assertEquals(format == PubSubDataSet.ValueFormat.CSV, beanUnderTest.acceptFormat(format),
                "CSVMessageGenerator must accept only CSV");
    }

    @Test
    public void generateTest() {
        dataSet.setValueFormat(PubSubDataSet.ValueFormat.CSV);
        dataSet.setFieldDelimiter(PubSubDataSet.CSVDelimiter.SEMICOLON);
        beanUnderTest.init(dataSet);

        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("name").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("age").withType(Schema.Type.INT).build())
                .withEntry(rbf.newEntryBuilder().withName("state").withType(Schema.Type.STRING).build())
                .withEntry(rbf.newEntryBuilder().withName("nullableString").withType(Schema.Type.STRING).withNullable(true).build())

                .build();
        Record record = rbf.newRecordBuilder(schema)
                .withString("name", "John Smith")
                .withInt("age", 42)
                .withString("state", "CA")
                .build();

        PubsubMessage message = beanUnderTest.generateMessage(record);
        Assertions.assertNotNull(message, "Message is null");
        System.out.println(message.getData().toStringUtf8());
    }
}
