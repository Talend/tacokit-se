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

package org.talend.components.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.bson.Document;
import org.bson.types.Binary;
import org.bson.types.ObjectId;
import org.talend.components.mongodb.utils.MongoDBTestExtension;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.text.MessageFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.talend.sdk.component.api.record.Schema.Type.*;

public class MongoTestBase {

    private final static String assertResultFormat = "Expected value for column {0} is {1}, but was {2}(expected object is {3} but was {4}";

    public Schema createSchema(RecordBuilderFactory recordBuilderFactory) {
        // No reason to check float, as mongodb saves it as double anyway.
        Schema.Builder schemaBuilder = recordBuilderFactory.newSchemaBuilder(Schema.Type.RECORD)
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col1").withType(INT).withNullable(false).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col2").withType(STRING).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col3").withType(LONG).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col4").withType(DOUBLE).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col5").withType(BOOLEAN).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col6").withType(DATETIME).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col7").withType(BYTES).build())
                .withEntry(recordBuilderFactory.newEntryBuilder().withName("col8").withType(ARRAY)
                        .withElementSchema(recordBuilderFactory.newSchemaBuilder(Schema.Type.INT).build()).build());

        return schemaBuilder.build();
    }

    public Record next(int currentCount, RecordBuilderFactory recordBuilderFactory, Schema schema) {
        Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        final Record.Builder builder = recordBuilderFactory.newRecordBuilder(schema);
        builder.withInt("col1", currentCount);
        builder.withString("col2", String.valueOf("artist" + currentCount));
        builder.withLong("col3", currentCount * 100);
        builder.withDouble("col4", currentCount / 3.0);
        builder.withBoolean("col5", currentCount % 2 == 0);
        builder.withDateTime("col6", new Date());
        builder.withBytes("col7", "String".getBytes());
        List<Integer> intList = Arrays.asList(currentCount, currentCount * 2, currentCount * 3);
        builder.withArray(entryBuilder.withName("col8").withType(ARRAY)
                .withElementSchema(recordBuilderFactory.newSchemaBuilder(INT).build()).build(), intList);

        return builder.build();
    }

    protected MongoClient createClient(MongoDBTestExtension.TestContext testContext) {
        MongoClientOptions.Builder builder = MongoClientOptions.builder();
        if (testContext.getDataStore().isUseSSL()) {
            builder.sslEnabled(true);
        }
        MongoClientOptions opts = builder.build();
        MongoClient client = new MongoClient(
                new ServerAddress(testContext.getDataStore().getServer(), testContext.getDataStore().getPort()),
                MongoCredential.createCredential(testContext.getDataStore().getUsername(),
                        testContext.getDataStore().getAuthenticationDatabase(),
                        testContext.getDataStore().getPassword().toCharArray()),
                opts);
        return client;
    }

    protected List<Record> convertToRecords(Iterator<Document> documentsIterator, Schema schema,
            RecordBuilderFactory recordBuilderFactory) {
        List<Record> result = new ArrayList<>();
        while (documentsIterator.hasNext()) {
            Document doc = documentsIterator.next();
            result.add(convertToRecord(doc, schema, recordBuilderFactory));
        }
        return result;
    }

    protected Record convertToRecord(Document doc, Schema schema, RecordBuilderFactory recordBuilderFactory) {
        Record.Builder recordBuilder = recordBuilderFactory.newRecordBuilder(schema);
        schema.getEntries().stream().map(Schema.Entry::getName)
                .forEach(c -> addColumn(recordBuilder, c, doc.get(c), recordBuilderFactory));
        return recordBuilder.build();
    }

    protected void addColumn(final Record.Builder builder, final String name, Object value,
            RecordBuilderFactory recordBuilderFactory) {
        final Schema.Entry.Builder entryBuilder = recordBuilderFactory.newEntryBuilder();
        entryBuilder.withName(name).withNullable(true);
        if (value instanceof ObjectId) {
            builder.withString(entryBuilder.withType(Schema.Type.STRING).build(), value == null ? null : value.toString());
        } else if (value instanceof String) {
            builder.withString(entryBuilder.withType(Schema.Type.STRING).build(), value == null ? null : (String) value);
        } else if (value instanceof Boolean) {
            builder.withBoolean(entryBuilder.withType(Schema.Type.BOOLEAN).build(), value == null ? null : (Boolean) value);
        } else if (value instanceof Date) {
            builder.withDateTime(entryBuilder.withType(Schema.Type.DATETIME).build(), value == null ? null : (Date) value);
        } else if (value instanceof Double) {
            builder.withDouble(entryBuilder.withType(Schema.Type.DOUBLE).build(), value == null ? null : (Double) value);
        } else if (value instanceof Integer) {
            builder.withInt(entryBuilder.withType(Schema.Type.INT).build(), value == null ? null : (Integer) value);
        } else if (value instanceof Long) {
            builder.withLong(entryBuilder.withType(Schema.Type.LONG).build(), value == null ? null : (Long) value);
        } else if (value instanceof Binary) {
            builder.withBytes(entryBuilder.withType(Schema.Type.BYTES).build(),
                    value == null ? null : ((Binary) value).getData());
        } else if (value instanceof byte[]) {
            builder.withBytes(entryBuilder.withType(Schema.Type.BYTES).build(), (byte[]) value);
        } else if (value instanceof List<?>) {
            builder.withArray(entryBuilder.withType(Schema.Type.ARRAY)
                    .withElementSchema(recordBuilderFactory.newSchemaBuilder(INT).build()).build(),
                    value == null ? null : (List<?>) value);
        } else {
            builder.withString(entryBuilder.withType(Schema.Type.STRING).build(), value == null ? null : value.toString());
        }
    }

    protected List<Record> createTestData(int count, RecordBuilderFactory recordBuilderFactory, Schema schema) {
        List<Record> result = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            result.add(next(i, recordBuilderFactory, schema));
        }
        return result;
    }

    protected List<Document> createDocuments(int count) {
        List<Document> result = new ArrayList<>();
        for (int currentCount = 0; currentCount < count; currentCount++) {
            Document doc = createDocument(currentCount);
            result.add(doc);
        }
        return result;
    }

    protected Document createDocument(int currentCount) {
        Document doc = new Document();
        doc.put("col1", currentCount);
        doc.put("col2", String.valueOf("artist" + currentCount));
        doc.put("col3", currentCount * 100L);
        doc.put("col4", currentCount / 3.0);
        doc.put("col5", currentCount % 2 == 0);
        doc.put("col6", new Date());
        doc.put("col7", ("String" + currentCount).getBytes());
        doc.put("col8", Arrays.asList(currentCount, currentCount * 3, currentCount * 5));
        return doc;
    }

    protected void assertResult(Schema schema, List<Record> testData, List<Record> result) {
        Map<Object, Record> testRecordsMap = testData.stream()
                .collect(Collectors.toMap(c -> c.get(Object.class, "col1"), c -> c));
        result.stream().forEach(c -> {
            Object col1 = c.get(Object.class, "col1");
            final Record expectedDoc = testRecordsMap.get(col1);
            schema.getEntries().forEach(r -> assertObject(c, expectedDoc, r.getName()));
        });
    }

    protected void assertObject(Record actual, Record expectedDoc, String colName) {
        if (actual.get(Object.class, colName) instanceof byte[]) {
            assertArrayEquals(
                    MessageFormat.format(assertResultFormat, colName, expectedDoc.get(Object.class, colName),
                            actual.get(Object.class, colName), expectedDoc, actual),
                    (byte[]) expectedDoc.get(Object.class, colName), (byte[]) actual.get(Object.class, colName));
        } else {
            assertEquals(
                    MessageFormat.format(assertResultFormat, colName, expectedDoc.get(Object.class, colName),
                            actual.get(Object.class, colName), expectedDoc, actual),
                    expectedDoc.get(Object.class, colName), actual.get(Object.class, colName));
        }
    }

}
