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

import java.util.Arrays;
import java.util.Iterator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CloseableIteratorTest {

    private boolean isClosed;

    @Test
    void next() {
        this.isClosed = false;
        final Iterator<Integer> iterator = Arrays.asList(1, 2, 3).iterator();
        final Iterator<Integer> closeableIterator = IteratorComposer.of(iterator).closeable(this::close).build();
        int k = 1;
        while (closeableIterator.hasNext()) {
            final Integer next = closeableIterator.next();
            Assertions.assertEquals(k, next);
            k++;
        }
        Assertions.assertEquals(true, this.isClosed);
        Assertions.assertEquals(4, k);
    }

    void close() {
        this.isClosed = true;
    }
}