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
package org.talend.components.common.collections;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

public class IteratorFilter<T> implements Iterator<T> {

    private final Iterator<T> primaryIterator;

    private final Predicate<T> filter;

    private T nextElement;

    public IteratorFilter(Iterator<T> primaryIterator, Predicate<T> filter) {
        this.primaryIterator = primaryIterator;
        this.filter = filter;
        this.nextElement = this.searchNext();
    }

    @Override
    public boolean hasNext() {
        return this.nextElement != null;
    }

    @Override
    public T next() {
        if (this.nextElement == null) {
            throw new NoSuchElementException("No more element for this iterator");
        }
        final T result = this.nextElement;
        this.nextElement = this.searchNext();
        return result;
    }

    private T searchNext() {
        while (this.primaryIterator.hasNext()) {
            final T next = this.primaryIterator.next();
            if (this.filter.test(next)) {
                return next;
            }
        }
        return null;
    }
}
