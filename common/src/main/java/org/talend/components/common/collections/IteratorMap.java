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

import java.util.Iterator;
import java.util.function.Function;

/**
 * Iterator with map function.
 *
 * @param <T> : type of primary iterator.
 * @param <U> : target type of this iterator.
 */
public class IteratorMap<T, U> implements Iterator<U> {

    /**
     * primary iterator
     */
    private final Iterator<T> primaryIterator;

    /**
     * transformed function
     */
    private final Function<T, U> mapFunction;

    /**
     * If true, null value are computed, else they are returned as is
     */
    private final boolean computeNull;

    public IteratorMap(Iterator<T> primaryIterator, Function<T, U> mapFunction) {
        this(primaryIterator, mapFunction, false);
    }

    public IteratorMap(Iterator<T> primaryIterator, Function<T, U> mapFunction, boolean computeNull) {
        this.primaryIterator = primaryIterator;
        this.mapFunction = mapFunction;
        this.computeNull = computeNull;
    }

    @Override
    public boolean hasNext() {
        return this.primaryIterator.hasNext();
    }

    @Override
    public U next() {
        final T primaryObject = this.primaryIterator.next();

        if (primaryObject == null && !computeNull) {
            return null;
        }

        return this.mapFunction.apply(primaryObject);
    }
}
