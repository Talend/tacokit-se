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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;

/**
 * Iterator that give all records of inner-iterator except n last-one.
 * Usefull when reading source with footer.
 */
public class DelayedIterator<T> implements Iterator<T> {

    private final Iterator<T> wrapped;

    private final List<T> tempElements;

    private final int sizeTempElement;

    private int tempIndex;

    public DelayedIterator(Iterator<T> wrapped, int nbeTempElement) {
        this.wrapped = wrapped;
        this.sizeTempElement = nbeTempElement;
        this.tempElements = new ArrayList<>(nbeTempElement);
        this.tempIndex = 0;
        this.init();
    }

    @Override
    public boolean hasNext() {
        // if wrapped has no more element mean tempElements array contains only footers.
        return this.wrapped.hasNext();
    }

    @Override
    public T next() {
        if (this.hasNext()) {
            T ret = tempElements.get(this.tempIndex);
            T nextTemp = this.wrapped.next();
            this.tempElements.set(this.tempIndex, nextTemp);
            this.tempIndex = (this.tempIndex + 1) % sizeTempElement;

            return ret;
        }
        return null;
    }

    private void init() {
        for (int i = 0; i < this.sizeTempElement; i++) {
            if (this.wrapped.hasNext()) {
                this.tempElements.add(this.wrapped.next());
            } else {
                this.tempElements.add(null);
            }
        }
    }
}
