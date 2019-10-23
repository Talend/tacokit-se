/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.workday.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

class BufferedProducerIteratorTest {

    @Test
    void next() {

        BufferedProducerIterator<Integer> buff = new BufferedProducerIterator<>(this::nextIter);

        List<Integer> liste = new ArrayList<>();
        Integer next = buff.next();
        while (next != null) {

            liste.add(next);
            if (liste.size() % 4 == 0) {
                attendre(Math.abs(rdm.nextInt(11)));
            }
            next = buff.next();
        }
        Assertions.assertEquals(liste.size(), 2400, "liste " + liste.size());

        for (int i = 0; i < liste.size(); i++) {
            Assertions.assertEquals(i, liste.get(i), "indice " + i + " = " + liste.get(i));
        }
    }

    private Random rdm = new Random(System.currentTimeMillis());

    public Iterator<Integer> nextIter(int page) {

        if (page >= 80) {
            return null;
        }
        IntStream flux = IntStream.range(page * 30, (page + 1) * 30);
        attendre(Math.abs(rdm.nextInt(30)));
        return flux.iterator();
    }

    private void attendre(int time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}