package org.talend.components.bigquery.output;

import com.google.api.services.bigquery.model.TableSchema;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.bigquery.service.BigQueryService;
import org.talend.components.bigquery.service.I18nMessage;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.internationalization.InternationalizationServiceFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import java.time.ZonedDateTime;
import java.util.*;

public class TacoKitRecordToTableRowConverterTest {

    @Test
    public void testNullDatetime() {
        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema.Entry arrayEntry = rbf.newEntryBuilder().withName("aDateTimeArray").withType(Schema.Type.ARRAY).withNullable(true)
                .withElementSchema(rbf.newSchemaBuilder(Schema.Type.DATETIME).build()).build();
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("aString").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(rbf.newEntryBuilder().withName("aDateTime").withType(Schema.Type.DATETIME).withNullable(true).build())
                .withEntry(arrayEntry)
                .build();

        com.google.cloud.bigquery.Schema tableSchema = new BigQueryService().convertToGoogleSchema(schema);

        I18nMessage i18n = new InternationalizationServiceFactory(() -> Locale.US).create(
                I18nMessage.class, Thread.currentThread().getContextClassLoader());

        TacoKitRecordToTableRowConverter converter = new TacoKitRecordToTableRowConverter(tableSchema, i18n);

        // Test, values not null
        Record record = rbf.newRecordBuilder(schema)
                .withString("aString", "notNull")
                .withDateTime("aDateTime", new Date())
                .withArray(arrayEntry, Arrays.asList(ZonedDateTime.now(), null, ZonedDateTime.now()))
                .build();
        Map<String, ?> map = converter.apply(record);

        Assertions.assertNotNull(map.get("aString"), "String must not be null");
        Assertions.assertNotNull(map.get("aDateTime"), "DateTime must not be null");
        Assertions.assertNotNull(map.get("aDateTimeArray"), "DateTimeArray must not be null");
        Assertions.assertEquals(3, ((Collection) map.get("aDateTimeArray")).size(),  "DateTimeArray must have 3 elements");

        // Values not set at all
        record = rbf.newRecordBuilder(schema)
                .build();
        map = converter.apply(record);

        Assertions.assertNull(map.get("aString"), "String must be null");
        Assertions.assertNull(map.get("aDateTime"), "DateTime must be null");
        Assertions.assertNull(map.get("aDateTimeArray"), "DateTimeArray must be null");

        // Values explicitly set at null
        record = rbf.newRecordBuilder(schema)
                .withString("aString", null)
                .withDateTime("aDateTime", (Date) null)
                .build();
        map = converter.apply(record);

        Assertions.assertNull(map.get("aString"), "String must be null (explicit)");
        Assertions.assertNull(map.get("aDateTime"), "DateTime must be null (explicit)");
        Assertions.assertNull(map.get("aDateTimeArray"), "DateTimeArray must be null");

    }
}
