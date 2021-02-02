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
package org.talend.components.common.stream.output.line;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.function.Consumer;

import org.talend.components.common.test.records.AssertionsBuilder;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import lombok.RequiredArgsConstructor;

public class LineValueBuilder implements AssertionsBuilder<List<String>> {

    private final List<String> fields = new ArrayList<>();

    @Override
    public void startRecord(final int id) {
        this.fields.clear();
    }

    @Override
    public void addField(final int id, final Schema.Entry field, final Object value) {

        if (field.getType() == Schema.Type.ARRAY) {
            return; // array are ignored.
        }
        if (field.getType() == Schema.Type.RECORD) {
            if (value != null) {
                final Record rec = (Record) value;
                for (final Schema.Entry subField : field.getElementSchema().getEntries()) {
                    final Object subValue = rec.get(Object.class, subField.getName());
                    this.addField(id, subField, subValue);
                }
            }
        } else if (value == null) {
            this.fields.add(null);
        } else if (field.getType() == Schema.Type.BYTES) {
            final String lineValue = new String(Base64.getEncoder().encode((byte[]) value));
            this.fields.add(lineValue);
        } else {
            this.fields.add(value.toString());
        }
    }

    @Override
    public Consumer<List<String>> endRecord(final int id, final Record record) {
        final List<String> newFields = new ArrayList<>(this.fields.size());
        newFields.addAll(this.fields);
        return new LineChecker(newFields);
    }

    @RequiredArgsConstructor
    static class LineChecker implements Consumer<List<String>> {

        private final List<String> expected;

        @Override
        public void accept(List<String> realValues) {
            assertEquals(realValues.size(), expected.size(), expected.toString());
            for (int i = 0; i < realValues.size(); i++) {
                assertEquals(realValues.get(i), expected.get(i), "Error on field " + i);
            }
        }
    }
}
