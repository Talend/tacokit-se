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

import java.util.Iterator;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
@Getter
public class IndexValue<T> {

    private final T value;

    private final int index;

    public static <T> Iterable<IndexValue<T>> from(final Iterable<T> original) {
        return () -> new IndexValueIterator<T>(original.iterator());
    }

    public static <T> Stream<IndexValue<T>> streamOf(final Iterable<T> original) {
        final Iterable<IndexValue<T>> iterable = IndexValue.from(original);
        return StreamSupport.stream(iterable.spliterator(), false);
    }

    public <U> IndexValue<U> map(final Function<T, U> transform) {
        final U transformedValue = transform.apply(this.getValue());
        return new IndexValue<>(transformedValue, this.getIndex());
    }

    public static <T, U> Function<IndexValue<T>, IndexValue<U>> wrap(final Function<T, U> transform) {
        return (IndexValue<T> x) -> x.map(transform);
    }

    @RequiredArgsConstructor
    private static class IndexValueIterator<T> implements Iterator<IndexValue<T>> {

        private final Iterator<T> delegate;

        private int currentIndex = 0;

        @Override
        public boolean hasNext() {
            return this.delegate.hasNext();
        }

        @Override
        public IndexValue<T> next() {
            final T next = this.delegate.next();
            return new IndexValue<>(next, this.currentIndex++);
        }
    }
}
