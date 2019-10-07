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
        Assertions.assertEquals(liste.size(), 2400, "liste " + liste.size()); ;

        for (int i = 0; i < liste.size(); i++) {
            Assertions.assertEquals(i, liste.get(i), "indice " + i + " = " + liste.get(i));
        }
    }



    private Random rdm = new Random(System.currentTimeMillis());


    public Iterator<Integer> nextIter(int page) {

        if (page >= 80) {
            return null;
        }
        IntStream flux = IntStream.range(page*30, (page+1)*30);
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