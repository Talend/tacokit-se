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
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Locale;
import java.util.Map;

public class TacoKitRecordToTableRowConverterTest {

    @Test
    public void testNullDatetime() {
        RecordBuilderFactory rbf = new RecordBuilderFactoryImpl(null);
        Schema.Entry arrayEntry = rbf.newEntryBuilder().withName("aDateTimeArray").withType(Schema.Type.ARRAY).withNullable(true)
                .withElementSchema(rbf.newSchemaBuilder(Schema.Type.DATETIME).build()).build();
        Schema schema = rbf.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(rbf.newEntryBuilder().withName("aString").withType(Schema.Type.STRING).withNullable(true).build())
                .withEntry(rbf.newEntryBuilder().withName("aDateTime").withType(Schema.Type.DATETIME).withNullable(true).build())
                .withEntry(arrayEntry).build();

        com.google.cloud.bigquery.Schema tableSchema = new BigQueryService().convertToGoogleSchema(schema);

        I18nMessage i18n = new InternationalizationServiceFactory(() -> Locale.US).create(I18nMessage.class,
                Thread.currentThread().getContextClassLoader());

        TacoKitRecordToTableRowConverter converter = new TacoKitRecordToTableRowConverter(tableSchema, i18n);

        // Test, values not null
        Record record = rbf.newRecordBuilder(schema).withString("aString", "notNull").withDateTime("aDateTime", new Date())
                .withArray(arrayEntry, Arrays.asList(ZonedDateTime.now(), null, ZonedDateTime.now())).build();
        Map<String, ?> map = converter.apply(record);

        Assertions.assertNotNull(map.get("aString"), "String must not be null");
        Assertions.assertNotNull(map.get("aDateTime"), "DateTime must not be null");
        Assertions.assertNotNull(map.get("aDateTimeArray"), "DateTimeArray must not be null");
        Assertions.assertEquals(3, ((Collection) map.get("aDateTimeArray")).size(), "DateTimeArray must have 3 elements");

        // Values not set at all
        record = rbf.newRecordBuilder(schema).build();
        map = converter.apply(record);

        Assertions.assertNull(map.get("aString"), "String must be null");
        Assertions.assertNull(map.get("aDateTime"), "DateTime must be null");
        Assertions.assertNull(map.get("aDateTimeArray"), "DateTimeArray must be null");

        // Values explicitly set at null
        record = rbf.newRecordBuilder(schema).withString("aString", null).withDateTime("aDateTime", (Date) null).build();
        map = converter.apply(record);

        Assertions.assertNull(map.get("aString"), "String must be null (explicit)");
        Assertions.assertNull(map.get("aDateTime"), "DateTime must be null (explicit)");
        Assertions.assertNull(map.get("aDateTimeArray"), "DateTimeArray must be null");

    }
}
