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

public class IteratorFlatmap<T, U> implements Iterator<U> {

    /** primary iterator */
    private final Iterator<T> primaryIterator;

    /** second (flat) iterator */
    private Iterator<U> subIterator;

    /** transformed function */
    private final Function<T, Iterator<U>> mapFunction;

    public IteratorFlatmap(Iterator<T> primaryIterator, Function<T, Iterator<U>> mapFunction) {
        this.primaryIterator = primaryIterator;
        this.mapFunction = mapFunction;
        this.nextSub();
    }

    @Override
    public boolean hasNext() {
        if (this.subIterator == null || !this.subIterator.hasNext()) {
            this.nextSub();
        }
        return this.subIterator != null && this.subIterator.hasNext();
    }

    @Override
    public U next() {
        if (!this.hasNext()) {
            return null;
        }
        return this.subIterator.next();
    }

    private void nextSub() {
        if (this.primaryIterator.hasNext()) {
            final T source = this.primaryIterator.next();
            this.subIterator = this.mapFunction.apply(source);
        } else {
            this.subIterator = null;
        }
    }
}
