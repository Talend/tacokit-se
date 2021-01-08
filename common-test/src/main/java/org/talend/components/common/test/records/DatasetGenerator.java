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
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

public class DatasetGenerator<T> {

    @RequiredArgsConstructor
    public static class DataSet<T> {

        @Getter
        private final Record record;

        private final Consumer<T> checker;

        public void check(final T realValues) {
            this.checker.accept(realValues);
        }
    }

    interface RecordGenerator {

        <T> DataSet<T> generateNext(AssertionsBuilder<T> assertionsBuilder);
    }

    private final RecordGenerator recordGenerator;

    private final AssertionsBuilder<T> assertionsBuilder;

    public DatasetGenerator(final RecordGenerator recordGenerator, final AssertionsBuilder<T> assertionsBuilder) {
        this.recordGenerator = recordGenerator;
        this.assertionsBuilder = assertionsBuilder;
    }

    public DatasetGenerator(final RecordBuilderFactory factory, final AssertionsBuilder<T> assertionsBuilder) {
        this(new DefaultRecordGenerator(factory), assertionsBuilder);
    }

    public Iterator<DataSet<T>> generate(int size) {
        return new RecordIterator<T>(size, this.recordGenerator, this.assertionsBuilder);
    }

    private static class RecordIterator<T> implements Iterator<DataSet<T>> {

        private final int size;

        private final RecordGenerator provider;

        private final AssertionsBuilder<T> assertionsBuilder;

        private int current = 0;

        public RecordIterator(int size, RecordGenerator provider, AssertionsBuilder<T> assertionsBuilder) {
            this.size = size;
            this.provider = provider;
            this.assertionsBuilder = assertionsBuilder;
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
            return this.provider.generateNext(this.assertionsBuilder);
        }
    }
}
