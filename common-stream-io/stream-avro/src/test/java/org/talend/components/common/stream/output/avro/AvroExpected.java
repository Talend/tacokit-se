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
package org.talend.components.common.stream.output.avro;

import java.nio.ByteBuffer;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Assertions;
import org.talend.components.common.test.records.AssertionsBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;

import lombok.RequiredArgsConstructor;

public class AvroExpected implements AssertionsBuilder<GenericRecord> {

    final List<Consumer<GenericRecord>> verifiers = new ArrayList<>();

    @Override
    public void startRecord(int id) {
        this.verifiers.clear();
    }

    @Override
    public void addField(int id, Entry field, Object value) {
        final Consumer<GenericRecord> verifier = (GenericRecord obj) -> this.checkContains(obj, field, value);
        this.verifiers.add(verifier);
    }

    @Override
    public Consumer<GenericRecord> endRecord(int id, Record record) {
        final List<Consumer<GenericRecord>> copy = new ArrayList<>(this.verifiers.size());
        copy.addAll(this.verifiers);
        return new AvroChecker(copy);
    }

    @RequiredArgsConstructor
    static class AvroChecker implements Consumer<GenericRecord> {

        final List<Consumer<GenericRecord>> verifiers;

        @Override
        public void accept(final GenericRecord genericRecord) {
            Assertions.assertNotNull(genericRecord);
            verifiers.forEach((Consumer<GenericRecord> verif) -> verif.accept(genericRecord));
        }
    }

    private void checkContains(final GenericRecord record, final Schema.Entry field, final Object expectedValue) {
        final Object value = record.get(field.getName());
        if (expectedValue == null) {
            Assertions.assertNull(value);
            return;
        }
        Assertions.assertNotNull(value);
        if (field.getType() == Schema.Type.RECORD) {
            Assertions.assertTrue(value instanceof GenericRecord);
            Assertions.assertTrue(expectedValue instanceof Record);
            final GenericRecord innerRecord = (GenericRecord) value;
            final Record expectedRecord = (Record) expectedValue;

            for (Schema.Entry subField : expectedRecord.getSchema().getEntries()) {
                final Object subExpectedValue = expectedRecord.get(Object.class, subField.getName());
                checkContains(innerRecord, subField, subExpectedValue);
            }

        } else if (field.getType() != Schema.Type.DATETIME && field.getType() != Schema.Type.BYTES) {
            Assertions.assertEquals(value.getClass(), expectedValue.getClass());
            Assertions.assertEquals(value, expectedValue);
        } else if (field.getType() == Schema.Type.DATETIME) {
            Assertions.assertEquals(((ZonedDateTime) expectedValue).toInstant().toEpochMilli(), (Long) value);
        } else { // Schema.Type.BYTES
            Assertions.assertTrue(value instanceof ByteBuffer);
            Assertions.assertArrayEquals(((ByteBuffer) value).array(), (byte[]) expectedValue);
        }
    }

}
