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
package org.talend.components.common.stream.output.line;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class DatasetGenerator {

    @AllArgsConstructor
    public static class DataSet {

        @Getter
        private final Record record;

        private final List<String> expectedValue;

        public void check(final List<String> realValues) {
            assertEquals(realValues.size(), expectedValue.size(), expectedValue.toString());
            for (int i = 0; i < realValues.size(); i++) {
                assertEquals(realValues.get(i), expectedValue.get(i), "Error on field " + i);
            }
        }
    }

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    private final Schema schema;

    private final Schema sub_record_schema;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.getDefault());

    public DatasetGenerator() {
        this.sub_record_schema = buildSubSchema();
        this.schema = this.buildSchema(this.sub_record_schema);
    }

    public Iterator<DataSet> generate(int size) {
        return new RecordIterator(size, this);
    }

    private Schema buildSubSchema() {
        return this.factory.newSchemaBuilder(Schema.Type.RECORD) //
                .withEntry(this.newEntry(Schema.Type.STRING, "rec_string")) //
                .withEntry(this.newEntry(Schema.Type.INT, "rec_int")) //
                .build();
    }

    private Schema buildSchema(Schema sub) {

        return this.factory.newSchemaBuilder(Schema.Type.RECORD) //
                .withEntry(this.newEntry(Schema.Type.STRING, "a_string")) //
                .withEntry(this.newEntry(Schema.Type.BOOLEAN, "a_boolean")) //
                .withEntry(this.newEntry(Schema.Type.INT, "a_int")) //
                .withEntry(this.newEntry(Schema.Type.LONG, "a_long")) //
                .withEntry(this.newEntry(Schema.Type.FLOAT, "a_float")) //
                .withEntry(this.newEntry(Schema.Type.DOUBLE, "a_double")) //
                .withEntry(this.newEntry(Schema.Type.DATETIME, "a_datetime")) //
                .withEntry(this.newEntry(Schema.Type.BYTES, "a_byte_array")) //
                .withEntry(this.newArrayEntry("a_string_array", this.factory.newSchemaBuilder(Schema.Type.STRING).build())) //
                .withEntry(this.newRecordEntry("a_record", sub)).build(); //

    }

    private Schema.Entry newEntry(Schema.Type type, String name) {
        return this.factory.newEntryBuilder().withName(name).withType(type).withNullable(true).build();
    }

    private Schema.Entry newRecordEntry(String name, Schema nested) {
        return this.factory.newEntryBuilder().withType(Schema.Type.RECORD).withName(name).withElementSchema(nested)
                .withNullable(true).build();
    }

    private Schema.Entry newArrayEntry(String name, Schema nested) {
        return this.factory.newEntryBuilder().withType(Schema.Type.ARRAY).withName(name).withNullable(true)
                .withElementSchema(nested).build();
    }

    private Schema.Entry findEntry(String name) {
        return this.schema.getEntries().stream() //
                .filter((Schema.Entry e) -> Objects.equals(e.getName(), name)) //
                .findFirst().orElse(null);
    }

    private DataSet createARecord(int i) {
        Record.Builder builder = this.factory.newRecordBuilder(this.schema);
        int current_null_field = i % 11;
        int toggle = i % 2;

        final List<String> expected = new ArrayList<>();

        this.fullData((String b) -> builder.withString("a_string", b), current_null_field == 1, expected, () -> "string_" + i);

        this.fullData((Boolean b) -> builder.withBoolean("a_boolean", b), current_null_field == 2, expected, () -> toggle == 0);

        this.fullData((Integer b) -> builder.withInt("a_int", b), current_null_field == 3, expected,
                () -> (toggle == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE);
        this.fullData((Long b) -> builder.withLong("a_long", b), current_null_field == 4, expected,
                () -> (toggle == 0) ? Long.MIN_VALUE : Long.MAX_VALUE);

        this.fullData((Float b) -> builder.withFloat("a_float", b), current_null_field == 5, expected,
                () -> (toggle == 0) ? Float.MIN_VALUE : Float.MAX_VALUE);
        this.fullData((Double b) -> builder.withDouble("a_double", b), current_null_field == 6, expected,
                () -> (toggle == 0) ? Double.MIN_VALUE : Double.MAX_VALUE);

        this.fullData((ZonedDateTime b) -> builder.withDateTime("a_datetime", b), current_null_field == 7, expected, () -> {
            final LocalDate date = LocalDate.parse("10/04/" + (2000 + i), formatter);
            return date.atStartOfDay(ZoneId.of("UTC"));
        });

        if (current_null_field != 8) {
            builder.withBytes("a_byte_array", ("index_" + i).getBytes());
            final String lineValue = new String(Base64.getEncoder().encode(("index_" + i).getBytes()));
            expected.add(lineValue);
        } else {
            expected.add(null);
        }

        // Array are ignored by CSV
        if (current_null_field != 9) {
            final Entry stringArray = this.findEntry("a_string_array");
            builder.withArray(stringArray, Arrays.asList("a", "b"));
        }
        if (current_null_field != 10) {
            final Record sub_record = this.factory.newRecordBuilder(this.sub_record_schema)
                    .withString("rec_string", "rec_string_" + i) //
                    .withInt("rec_int", i) //
                    .build();
            builder.withRecord("a_record", sub_record);
            expected.add("rec_string_" + i);
            expected.add(Integer.toString(i));
        }

        final Record rec = builder.build();
        return new DataSet(rec, expected);
    }

    private <T> void fullData(Consumer<T> builder, boolean isNull, List<String> expected, Supplier<T> valueGetter) {

        if (isNull) {
            expected.add(null);
        } else {
            final T value = valueGetter.get();
            if (value != null) {
                builder.accept(value);
                expected.add(value.toString());
            } else {
                expected.add(null);
            }
        }
    }

    private static class RecordIterator implements Iterator<DataSet> {

        private final int size;

        private final DatasetGenerator provider;

        private int current = 0;

        public RecordIterator(int size, DatasetGenerator provider) {
            this.size = size;
            this.provider = provider;
        }

        @Override
        public boolean hasNext() {
            return current < size;
        }

        @Override
        public DataSet next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("iterator end already reached.");
            }
            current++;
            return this.provider.createARecord(current);
        }
    }
}
