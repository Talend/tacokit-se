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
package org.talend.components.common.stream.output.parquet;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.stream.input.parquet.ParquetRecordReader;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.record.Schema.Type;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import lombok.RequiredArgsConstructor;

class ParquetRecordWriterTest {

    @BeforeAll
    static void initLog() {
        System.setProperty("org.slf4j.simpleLogger.log.org.talend.components", "debug");
    }

    @ParameterizedTest
    @MethodSource("provideRecords")
    void write(final List<Record> records) throws IOException {
        final URL out = Thread.currentThread().getContextClassLoader().getResource("out");
        File fileOut = new File(out.getPath(), "fic1.parquet");
        if (fileOut.exists()) {
            fileOut.delete();
        }
        Path path = new Path(fileOut.getPath());
        final TCKParquetWriterBuilder builder = new TCKParquetWriterBuilder(path);

        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final Schema schema = records.get(0).getSchema();
        try (final ParquetWriter<Record> writer = builder.withSchema(schema).build()) {
            for (Record rec : records) {
                writer.write(rec);
            }
        }

        final ParquetRecordReader reader = new ParquetRecordReader(factory);

        final HadoopInputFile inputFile = HadoopInputFile.fromPath(path, new org.apache.hadoop.conf.Configuration());
        final Iterator<Record> recordIterator = reader.read(inputFile);
        final Iterator<Record> inputRecordIterator = records.iterator();
        while (recordIterator.hasNext()) {
            Assertions.assertTrue(inputRecordIterator.hasNext());

            final Record record = recordIterator.next();
            Assertions.assertNotNull(record);

            final Record refRecord = inputRecordIterator.next();
            Assertions.assertEquals(refRecord.getSchema(), record.getSchema());
            final Result areEquals = this.areEquals(record, refRecord, "");
            Assertions.assertTrue(areEquals.ok, "Fields not equals " + areEquals.field + " ==> " + record);
        }
    }

    private static Stream<Arguments> provideRecords() {
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

        final Schema.Entry f1 = factory.newEntryBuilder().withName("f1").withRawName("raw").withType(Schema.Type.STRING)
                .withNullable(false).build();
        final Schema.Entry f2 = factory.newEntryBuilder().withName("f2").withType(Schema.Type.INT).withNullable(false).build();
        final Schema.Entry f3 = factory.newEntryBuilder().withName("f3").withType(Schema.Type.BYTES).withNullable(true).build();
        final Schema.Entry f4 = factory.newEntryBuilder().withName("f4").withType(Schema.Type.DOUBLE).withNullable(true).build();
        final Schema.Entry f5 = factory.newEntryBuilder().withName("f5").withType(Type.DATETIME).withNullable(true).build();

        // simple
        final Schema schema = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(f1).withEntry(f2).withEntry(f3).withEntry(f4)
                .withEntry(f5).build();
        final Record record1 = factory.newRecordBuilder(schema).withString(f1, "value1").withInt(f2, 11).build();
        final Record record2 = factory.newRecordBuilder(schema).withString(f1, "value2").withInt(f2, 21)
                .withBytes(f3, "Hello".getBytes(StandardCharsets.UTF_8)).withDouble(f4, 0.23D)
                .withDateTime(f5, ZonedDateTime.of(LocalDateTime.of(2021, 5, 12, 15, 0, 50), ZoneId.of("UTC"))).build();
        final List<Record> records = Arrays.asList(record1, record2);

        // sub object
        final Schema.Entry fsub = factory.newEntryBuilder().withName("fsub") //
                .withType(Schema.Type.RECORD).withRawName("rawname") //
                .withElementSchema(schema).build();
        final Schema schema1 = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(fsub).build();
        final Record record3 = factory.newRecordBuilder(schema1).withRecord(fsub, record1).build();
        final List<Record> recordsWithSub = Arrays.asList(record3);

        // Array of privmitiv
        final Schema.Entry farray = factory.newEntryBuilder().withName("farray") //
                .withType(Schema.Type.ARRAY).withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build())
                .withNullable(true).build();
        final Schema schema2 = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(farray).build();
        final Record record4 = factory.newRecordBuilder(schema2).withArray(farray, Arrays.asList("a", "b", "c")).build();
        final List<Record> recordsWithArray = Arrays.asList(record4);

        // Array of records.
        final Schema.Entry farrayRec = factory.newEntryBuilder().withName("farray") //
                .withType(Schema.Type.ARRAY).withElementSchema(schema).withNullable(true).build();
        final Schema schemaArray = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(farrayRec).build();
        final Record record5 = factory.newRecordBuilder(schemaArray).withArray(farray, Arrays.asList(record1, record2)).build();
        final List<Record> recordsWithArrayRec = Arrays.asList(record5);

        // More complex record.
        final Entry e1 = factory.newEntryBuilder().withName("e1").withType(Schema.Type.RECORD).withElementSchema(schemaArray)
                .build();
        final Entry e2 = factory.newEntryBuilder().withName("e2").withType(Schema.Type.RECORD).withElementSchema(schema2).build();
        final Entry e3 = factory.newEntryBuilder().withName("e3").withType(Schema.Type.RECORD).withElementSchema(schema).build();
        final Schema schema3 = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(e1).withEntry(e2).withEntry(e3).build();
        final Record complexeRecord = factory.newRecordBuilder(schema3).withRecord(e1, record5).withRecord(e2, record4)
                .withRecord(e3, record1).build();

        // Array of Array of primitiv
        final Schema arrayOfInt = factory.newSchemaBuilder(Schema.Type.ARRAY)
                .withElementSchema(factory.newSchemaBuilder(Schema.Type.STRING).build()).build();
        final Entry arrayEntry = factory.newEntryBuilder().withName("array").withType(Schema.Type.ARRAY)
                .withElementSchema(arrayOfInt).withNullable(true).build();
        final Schema schema4 = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(arrayEntry).build();
        Collection<Collection<String>> intValues = Arrays.asList(Arrays.asList("AAA", "BBB"), Arrays.asList("CCC", "4", "5"),
                Arrays.asList("FFF"));
        final Record recordWithArrayOfArray = factory.newRecordBuilder(schema4).withArray(arrayEntry, intValues).build();

        // Array of array of record
        final Schema arrayOfRecord = factory.newSchemaBuilder(Schema.Type.ARRAY).withElementSchema(schema).build();
        final Entry arrayRecEntry = factory.newEntryBuilder().withName("array").withType(Schema.Type.ARRAY)
                .withElementSchema(arrayOfRecord).withNullable(true).build();
        final Schema schema5 = factory.newSchemaBuilder(Schema.Type.RECORD).withEntry(arrayRecEntry).build();
        final Record recordWithRecordArrayArray = factory.newRecordBuilder(schema5)
                .withArray(arrayRecEntry, Arrays.asList(Arrays.asList(record1), Arrays.asList(record2))).build();

        return Stream.of(Arguments.of(records), // simple
                Arguments.of(recordsWithSub), // with sub records
                Arguments.of(recordsWithArray), // with primitiv array
                Arguments.of(recordsWithArrayRec), // with array of rec
                Arguments.of(Arrays.asList(complexeRecord)), // higher complex record
                Arguments.of(Arrays.asList(recordWithArrayOfArray)), // array of array of primitiv
                Arguments.of(Arrays.asList(recordWithRecordArrayArray))); // array of array of record.
    }

    @RequiredArgsConstructor
    static class Result {

        public final boolean ok;

        public final String field;

        public Result merge(Result other) {
            if (this.ok) {
                return other;
            }
            return this;
        }

        public static final Result OK = new Result(true, "");
    }

    private Result areEquals(final Record r1, final Record r2, String field) {
        if (r1 == r2) {
            return Result.OK;
        }
        if (r1 == null || r2 == null) {
            return new Result(false, field);
        }
        if (!Objects.equals(r1.getSchema(), r2.getSchema())) {
            return new Result(false, field);
        }
        return r1.getSchema().getEntries().stream() //
                .map((Schema.Entry entry) -> {
                    final Object o1 = r1.get(Object.class, entry.getName());
                    final Object o2 = r2.get(Object.class, entry.getName());
                    return this.areFieldEquals(o1, o2, field + "." + entry.getName());
                }) //
                .reduce(Result.OK, Result::merge);
    }

    private Result areFieldEquals(final Object o1, Object o2, String field) {

        if (o1 == o2) {
            return Result.OK;
        }
        if (o1 == null || o2 == null) {
            return new Result(false, field);
        }
        if (o1 instanceof Record && o2 instanceof Record) {
            return this.areEquals((Record) o1, (Record) o2, field);
        }
        if (o1 instanceof Collection && o2 instanceof Collection) {
            final Collection<?> c1 = (Collection<?>) o1;
            final Collection<?> c2 = (Collection<?>) o2;
            if (c1.size() != c2.size()) {
                return new Result(false, field + ".Size");
            }
            final Iterator<?> iterator1 = c1.iterator();
            final Iterator<?> iterator2 = c2.iterator();
            int index = 1;
            while (iterator1.hasNext()) {
                final Object item1 = iterator1.next();
                final Object item2 = iterator2.next();
                final Result result = this.areFieldEquals(item1, item2, field + "[" + index + "]");
                if (!result.ok) {
                    return result;
                }
                index++;
            }
            return Result.OK;
        }
        if (o1 instanceof byte[] && o2 instanceof byte[]) {
            final boolean arrayEquals = Arrays.equals((byte[]) o1, (byte[]) o2);
            if (!arrayEquals) {
                return new Result(false, field + " (byte[])");
            }
            return Result.OK;
        }
        if (!Objects.equals(o1, o2)) {
            return new Result(false, field);
        }
        return Result.OK;
    }
}