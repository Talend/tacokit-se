package org.talend.components.workday.input;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.sdk.component.api.base.BufferizedProducerSupport;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;

class BufferedProducerIteratorTest {

    @Test
    void t2() {
        this.current = 0;
        BufferizedProducerSupport<Integer> s1 = new BufferizedProducerSupport<>(this::nextIter);

        List<Integer> liste = new ArrayList<>();
        Integer next = s1.next();
        while (next != null) {
            liste.add(next);
            if (liste.size() % 4 == 0) {
                attendre(Math.abs(rdm.nextInt(11)));
            }
            next = s1.next();
        }
        Assertions.assertTrue(liste.size() >= 1500, "liste " + liste.size()) ;
        Assertions.assertTrue(liste.size() <= 1530, "liste " + liste.size()) ;
        for (int i = 0; i < liste.size(); i++) {
            Assertions.assertEquals(i, liste.get(i), "indice " + i + " = " + liste.get(i));
        }
    }

    @Test
    void next() {
        this.current = 0;
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
        Assertions.assertTrue(liste.size() >= 1500, "liste " + liste.size()) ;
        Assertions.assertTrue(liste.size() <= 1530, "liste " + liste.size()) ;
        for (int i = 0; i < liste.size(); i++) {
            Assertions.assertEquals(i, liste.get(i), "indice " + i + " = " + liste.get(i));
        }
    }

    private int current;

    private Random rdm = new Random(System.currentTimeMillis());



    public Iterator<Integer> nextIter() {

        if (current >= 1500) {
            return null;
        }
        int nbe = Math.abs(rdm.nextInt(29)) + 1;
        IntStream flux = IntStream.range(current, current + nbe);
        current += nbe;
        attendre(Math.abs(rdm.nextInt(30)));

        return flux.iterator();
    }

    private void attendre(int time) {
        try {
            Thread.sleep(time);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}