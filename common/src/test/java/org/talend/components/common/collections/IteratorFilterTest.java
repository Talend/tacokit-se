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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class IteratorFilterTest {

    @Test
    void next() {
        final IteratorFilter<String> it1 = new IteratorFilter<>(Collections.emptyIterator(), Objects::nonNull);
        Assertions.assertFalse(it1.hasNext());
        try {
            it1.next();
            Assertions.fail("should throw NoSuchElementException");
        } catch (NoSuchElementException ex) {
        }

        final IteratorFilter<String> it2 = new IteratorFilter<>(Arrays.asList(null, "V1", null, "V2", null, null).iterator(),
                Objects::nonNull);
        List<String> ls = new ArrayList<>(2);
        it2.forEachRemaining(ls::add);
        Assertions.assertEquals(2, ls.size());
        Assertions.assertEquals("V1", ls.get(0));
        Assertions.assertEquals("V2", ls.get(1));

        final IteratorFilter<String> it3 = new IteratorFilter<>(Arrays.<String> asList(null, null, null).iterator(),
                Objects::nonNull);
        Assertions.assertFalse(it3.hasNext());
    }
}