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