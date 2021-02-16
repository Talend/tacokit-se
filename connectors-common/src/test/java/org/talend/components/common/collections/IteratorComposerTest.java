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
import java.util.Iterator;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IteratorComposerTest {

    @Test
    public void map() {
        final List<String> strings = Arrays.asList("Hello", "all", "of", "you");
        final Iterator<Integer> lengths = IteratorComposer.of(strings.iterator()).map(String::length).build();
        for (int i = 0; i < 6; i++) {
            Assertions.assertTrue(lengths.hasNext(), "hasnext not re-entrant");
        }
        this.checkNext(lengths, 5, 1);
        this.checkNext(lengths, 3, 2);
        this.checkNext(lengths, 2, 3);
        this.checkNext(lengths, 3, 4);
        Assertions.assertFalse(lengths.hasNext(), "no end");
    }

    @Test
    public void flatmap() {
        final List<String> strings = Arrays.asList("Hello;all", "of", "you", "", "see;you;later");
        final Iterator<String> iterator = IteratorComposer.of(strings.iterator()).flatmapIterable(this::cut).build();
        for (int i = 0; i < 6; i++) {
            Assertions.assertTrue(iterator.hasNext(), "hasnext not re-entrant");
        }
        int i = 1;
        this.checkNext(iterator, "Hello", i++);
        this.checkNext(iterator, "all", i++);
        this.checkNext(iterator, "of", i++);
        this.checkNext(iterator, "you", i++);
        this.checkNext(iterator, "", i++);
        this.checkNext(iterator, "see", i++);
        this.checkNext(iterator, "you", i++);
        this.checkNext(iterator, "later", i++);
    }

    @Test
    public void footer() {
        final List<String> strings = Arrays.asList("Hello", "all", "of", "you");
        final Iterator<String> it1 = IteratorComposer.of(strings.iterator()).skipFooter(0).build();
        for (int i = 0; i < 4; i++) {
            this.checkNext(it1, strings.get(i), i);
        }
        Assertions.assertFalse(it1.hasNext());

        final Iterator<String> it2 = IteratorComposer.of(strings.iterator()).skipFooter(2).build();
        for (int i = 0; i < 2; i++) {
            this.checkNext(it2, strings.get(i), i);
        }
        Assertions.assertFalse(it2.hasNext());
    }

    private Iterable<String> cut(String data) {
        return Arrays.asList(data.split(";"));
    }

    private <T> void checkNext(Iterator<T> iter, T value, int indice) {
        Assertions.assertTrue(iter.hasNext(), () -> "no next for " + indice);
        T real = iter.next();
        Assertions.assertEquals(value, real, "no equals for " + indice);
    }

}