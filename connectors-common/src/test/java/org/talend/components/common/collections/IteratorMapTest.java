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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

class IteratorMapTest {

    @Test
    void noNull() {
        final List<String> strings = Arrays.asList("Hello", "all", "of", "you");
        final IteratorMap<String, Integer> it = new IteratorMap<>(strings.iterator(), String::length);

        Assertions.assertEquals(5, it.next());
        Assertions.assertEquals(3, it.next());
        Assertions.assertEquals(2, it.next());
        Assertions.assertEquals(3, it.next());

        Assertions.assertFalse(it.hasNext());
    }

    @Test
    void nullNotComputed() {
        final List<String> strings = Arrays.asList("Hello", null, "all", null, "of", null, "you");
        final IteratorMap<String, Integer> it = new IteratorMap<>(strings.iterator(), s -> s.length());

        Assertions.assertEquals(5, it.next());
        Assertions.assertEquals(null, it.next());
        Assertions.assertEquals(3, it.next());
        Assertions.assertEquals(null, it.next());
        Assertions.assertEquals(2, it.next());
        Assertions.assertEquals(null, it.next());
        Assertions.assertEquals(3, it.next());

        Assertions.assertFalse(it.hasNext());
    }

    @Test
    void nullComputed() {
        final List<String> strings = Arrays.asList("Hello", null, "all", null, "of", null, "you");
        final IteratorMap<String, Integer> it = new IteratorMap<>(strings.iterator(), s -> s == null ? -1 : s.length(), true);

        Assertions.assertEquals(5, it.next());
        Assertions.assertEquals(-1, it.next());
        Assertions.assertEquals(3, it.next());
        Assertions.assertEquals(-1, it.next());
        Assertions.assertEquals(2, it.next());
        Assertions.assertEquals(-1, it.next());
        Assertions.assertEquals(3, it.next());

        Assertions.assertFalse(it.hasNext());
    }

}