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
package org.talend.components.common.collections;

import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator composition.
 * 
 * @param <T> : iterating type.
 */
public class IteratorComposer<T> {

    /** inner iterator */
    private final Iterator<T> iterator;

    private IteratorComposer(Iterator<T> iterator) {
        this.iterator = iterator;
    }

    /**
     * Builder.
     * 
     * @param iterator : primary inner iterator.
     * @param <T> : iterating type.
     * @return iterator composer.
     */
    public static <T> IteratorComposer<T> of(Iterator<T> iterator) {
        return new IteratorComposer(iterator);
    }

    /**
     * Composed iterator, add a mapping function for each element of iterator.
     * 
     * @param mapFunction : convert primary type in target type.
     * @param <U> target iterating type.
     * @return iterator composer.
     */
    public <U> IteratorComposer<U> map(Function<T, U> mapFunction) {
        final Iterator<U> composedIterator = new IteratorMap<>(this.iterator, mapFunction);
        return IteratorComposer.of(composedIterator);
    }

    public IteratorComposer<T> skipFooter(int footerSize) {
        if (footerSize <= 0) {
            return this;
        }
        final Iterator<T> delayedIterator = new DelayedIterator<>(this.iterator, footerSize);
        return IteratorComposer.of(delayedIterator);
    }

    public IteratorComposer<T> closeable(AutoCloseable closeFunction) {
        final Iterator<T> closeableIterator = new CloseableIterator<>(this.iterator, closeFunction);
        return IteratorComposer.of(closeableIterator);
    }

    /**
     * Composed iterator with flat map function.
     * 
     * @param mapFunction : function that return iterator on type U for each elements of first iterator
     * @param <U> : target type.
     * @return iterator composer.
     */
    public <U> IteratorComposer<U> flatmap(Function<T, Iterator<U>> mapFunction) {
        final Iterator<U> composedIterator = new IteratorFlatmap<>(this.iterator, mapFunction);
        return IteratorComposer.of(composedIterator);
    }

    /**
     * Composed iterator with flat map function to iterable.
     * 
     * @param mapFunction : function that return iterator on type U for each elements of first iterator
     * @param <U> : target type.
     * @return iterator composer.
     */
    public <U> IteratorComposer<U> flatmapIterable(Function<T, Iterable<U>> mapFunction) {
        Function<T, Iterator<U>> iteratorFunction = this.convertFunction(mapFunction);
        return this.flatmap(iteratorFunction);
    }

    private <U1, U2> Function<U1, Iterator<U2>> convertFunction(Function<U1, Iterable<U2>> primaryFunction) {
        return new Function<U1, Iterator<U2>>() {

            @Override
            public Iterator<U2> apply(U1 u1) {
                final Iterable<U2> res = primaryFunction.apply(u1);
                if (res == null) {
                    return Collections.emptyIterator();
                }
                return res.iterator();
            }
        };
    }

    public Iterator<T> build() {
        return this.iterator;
    }
}
