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
package org.talend.components.common.test.records;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Locale;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;

public class DatasetGenerator<T> {

    @AllArgsConstructor
    public static class DataSet<T> {

        @Getter
        private final Record record;

        private final Consumer<T> checker;

        public void check(final T realValues) {
            this.checker.accept(realValues);
        }
    }

    private final ExpectedValueBuilder<T> expectedValueBuilder;

    private final RecordBuilderFactory factory;

    private final Schema schema;

    private final Schema sub_record_schema;

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.getDefault());

    public DatasetGenerator(final RecordBuilderFactory factory,
            final ExpectedValueBuilder<T> expectedValueBuilder) {
        this.factory = factory;
        this.expectedValueBuilder = expectedValueBuilder;
        this.sub_record_schema = buildSubSchema();
        this.schema = this.buildSchema(this.sub_record_schema);
    }

    public Iterator<DataSet<T>> generate(int size) {
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
        this.expectedValueBuilder.startRecord(i);
        int current_null_field = i % 11;
        int toggle = i % 2;

        this.fullData(i, builder::withString, "a_string", current_null_field == 1, () -> "string_" + i);
        this.fullData(i, builder::withBoolean, "a_boolean", current_null_field == 2, () -> toggle == 0);
        this.fullData(i, builder::withInt, "a_int", current_null_field == 3,
                () -> (toggle == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE);
        this.fullData(i, builder::withLong, "a_long", current_null_field == 4,
                () -> (toggle == 0) ? Long.MIN_VALUE : Long.MAX_VALUE);
        this.fullData(i, builder::withFloat, "a_float", current_null_field == 5,
                () -> (toggle == 0) ? Float.MIN_VALUE : Float.MAX_VALUE);
        this.fullData(i, builder::withDouble, "a_double", current_null_field == 6,
                () -> (toggle == 0) ? Double.MIN_VALUE : Double.MAX_VALUE);
        this.fullData(i, builder::withDateTime, "a_datetime", current_null_field == 7, () -> {
            final LocalDate date = LocalDate.parse("10/04/" + (2000 + i), formatter);
            return date.atStartOfDay(ZoneId.of("UTC"));
        });

        this.fullData(i, builder::withBytes, "a_byte_array", current_null_field == 8, () -> ("index_" + i).getBytes());

        this.fullData(i, builder::withArray, "a_string_array", current_null_field == 9, () -> Arrays.asList("a", "b"));

        this.fullData(i, builder::withRecord, "a_record", current_null_field == 10,
                () -> this.factory.newRecordBuilder(this.sub_record_schema).withString("rec_string", "rec_string_" + i) //
                        .withInt("rec_int", i) //
                        .build());

        final Record rec = builder.build();
        final Consumer<T> expected = this.expectedValueBuilder.endRecord(i, rec);
        return new DataSet(rec, expected);
    }

    private <U> void fullData(int indice, BiConsumer<Schema.Entry, U> builder, String fieldName, boolean isNull,
            Supplier<U> valueGetter) {

        final Schema.Entry field = this.findEntry(fieldName);
        if (isNull) {
            this.expectedValueBuilder.addField(indice, field, null);
        } else {
            final U value = valueGetter.get();
            if (value != null) {
                this.expectedValueBuilder.addField(indice, field, value);
                builder.accept(field, value);
            } else {
                this.expectedValueBuilder.addField(indice, field, null);
            }
        }
    }

    private static class RecordIterator<T> implements Iterator<DataSet<T>> {

        private final int size;

        private final DatasetGenerator<T> provider;

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
        public DataSet<T> next() {
            if (!this.hasNext()) {
                throw new NoSuchElementException("iterator end already reached.");
            }
            current++;
            return this.provider.createARecord(current);
        }
    }
}
