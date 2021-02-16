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

import java.util.Arrays;
import java.util.List;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DelayedIteratorTest {

    @Test
    void next() {
        final List<Integer> integers = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        final DelayedIterator<Integer> delayed = new DelayedIterator<>(integers.iterator(), 2);

        for (int i = 1; i <= 20; i++) {
            Assertions.assertTrue(delayed.hasNext());
        }

        for (int i = 1; i <= 8; i++) {
            Assertions.assertTrue(delayed.hasNext());
            Assertions.assertEquals(i, delayed.next());
        }
        Assertions.assertFalse(delayed.hasNext());

        final List<Integer> shortList = Arrays.asList(1, 2, 3);
        final DelayedIterator<Integer> delayed1 = new DelayedIterator<>(shortList.iterator(), 4);
        Assertions.assertFalse(delayed1.hasNext());

    }
}