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
package org.talend.components.common.test.records;

import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Locale;
import java.util.Objects;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.talend.components.common.test.records.DatasetGenerator.DataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Record.Builder;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

public class DefaultRecordGenerator implements DatasetGenerator.RecordGenerator {

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MM/yyyy", Locale.getDefault());

    private final RecordBuilderFactory factory;

    private final Schema schema;

    private final Schema subRecordSchema;

    private int index = 1;

    public DefaultRecordGenerator(final RecordBuilderFactory factory) {
        this.factory = factory;
        this.subRecordSchema = buildSubSchema();
        this.schema = this.buildSchema(this.subRecordSchema);
    }

    @Override
    public <T> DataSet<T> generateNext(final AssertionsBuilder<T> assertionsBuilder) {
        Builder builder = this.factory.newRecordBuilder(this.schema);
        assertionsBuilder.startRecord(index);
        int currentNullField = index % 11;
        int toggle = index % 2;

        this.fullData(assertionsBuilder, builder::withString, "a_string", currentNullField == 1, () -> "string_" + index);
        this.fullData(assertionsBuilder, builder::withBoolean, "a_boolean", currentNullField == 2, () -> toggle == 0);
        this.fullData(assertionsBuilder, builder::withInt, "a_int", currentNullField == 3,
                () -> (toggle == 0) ? Integer.MIN_VALUE : Integer.MAX_VALUE);
        this.fullData(assertionsBuilder, builder::withLong, "a_long", currentNullField == 4,
                () -> (toggle == 0) ? Long.MIN_VALUE : Long.MAX_VALUE);
        this.fullData(assertionsBuilder, builder::withFloat, "a_float", currentNullField == 5,
                () -> (toggle == 0) ? Float.MIN_VALUE : Float.MAX_VALUE);
        this.fullData(assertionsBuilder, builder::withDouble, "a_double", currentNullField == 6,
                () -> (toggle == 0) ? Double.MIN_VALUE : Double.MAX_VALUE);
        this.fullData(assertionsBuilder, builder::withDateTime, "a_datetime", currentNullField == 7, () -> {
            final LocalDate date = LocalDate.parse("10/04/" + (2000 + index), formatter);
            return date.atStartOfDay(ZoneId.of("UTC"));
        });

        this.fullData(assertionsBuilder, builder::withBytes, "a_byte_array", currentNullField == 8, ("index_" + index)::getBytes);

        this.fullData(assertionsBuilder, builder::withArray, "a_string_array", currentNullField == 9,
                () -> Arrays.asList("a", "b"));

        this.fullData(assertionsBuilder, builder::withRecord, "a_record", currentNullField == 10,
                () -> this.factory.newRecordBuilder(this.subRecordSchema).withString("rec_string", "rec_string_" + index) //
                        .withInt("rec_int", index) //
                        .build());

        final Record rec = builder.build();
        final Consumer<T> expected = assertionsBuilder.endRecord(this.index, rec);
        this.index++;
        return new DataSet<>(rec, expected);
    }

    private <U, T> void fullData(AssertionsBuilder<T> assertionsBuilder, BiConsumer<Entry, U> builder, String fieldName,
            boolean isNull, Supplier<U> valueGetter) {

        final Schema.Entry field = this.findEntry(fieldName);
        if (isNull) {
            assertionsBuilder.addField(index, field, null);
        } else {
            final U value = valueGetter.get();
            if (value != null) {
                assertionsBuilder.addField(index, field, value);
                builder.accept(field, value);
            } else {
                assertionsBuilder.addField(index, field, null);
            }
        }
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

    private Schema.Entry newEntry(final Schema.Type type, final String name) {
        return this.startEntry(name, type).build();
    }

    private Schema.Entry newRecordEntry(final String name, final Schema nested) {
        return this.startEntry(name, Schema.Type.RECORD).withElementSchema(nested).build();
    }

    private Schema.Entry newArrayEntry(final String name, final Schema nested) {
        return this.startEntry(name, Schema.Type.ARRAY).withElementSchema(nested).build();
    }

    private Schema.Entry.Builder startEntry(final String name, final Schema.Type type) {
        return this.factory.newEntryBuilder().withName(name).withType(type).withNullable(true);
    }

    private Schema.Entry findEntry(final String name) {
        return this.schema.getEntries().stream() //
                .filter((Schema.Entry e) -> Objects.equals(e.getName(), name)) //
                .findFirst().orElse(null);
    }
}
