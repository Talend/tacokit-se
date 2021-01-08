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

import java.util.Iterator;
import java.util.function.Consumer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.common.test.records.DatasetGenerator.DataSet;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;
import org.talend.sdk.component.runtime.record.RecordBuilderFactoryImpl;

class DatasetGeneratorTest {

    private final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");

    @ParameterizedTest
    @MethodSource("testString")
    void testOK(DataSet<String> ds) {
        final String value = ds.getRecord().getOptionalString("a_string").orElse("oother");
        ds.check(value);
    }

    private static Iterator<DataSet<String>> testString() {
        final AssertionsBuilder<String> checkBuilder = new TrueBuilder();
        final RecordBuilderFactory factory = new RecordBuilderFactoryImpl("test");
        final DatasetGenerator<String> generator = new DatasetGenerator<>(factory, checkBuilder);
        return generator.generate(40);
    }

    static class TrueBuilder implements AssertionsBuilder<String> {

        @Override
        public void startRecord(int id) {
        }

        @Override
        public void addField(int id, Entry field, Object value) {
        }

        @Override
        public Consumer<String> endRecord(int id, Record record) {
            return (String f) -> Assertions.assertNotNull(f);
        }
    }
}