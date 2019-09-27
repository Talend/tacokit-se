package org.talend.components.workday.input;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;


public class BufferedProducerIterator<T> {

    private final ExecutorService exe = Executors.newFixedThreadPool(2);

    private final Supplier<Iterator<T>> supplier;

    private Iterator<T>[] currents = new Iterator[2];

    CompletableFuture[] cp = new CompletableFuture[2];

    private final ReentrantReadWriteLock[] lock = new ReentrantReadWriteLock[2];

    private int current = 0;

    public BufferedProducerIterator(Supplier<Iterator<T>> supplier) {
        this.supplier = supplier;
        this.currents[0] = this.supplier.get();
        this.currents[1] = null;
        this.current = 0;
        lock[0] = new ReentrantReadWriteLock();
        lock[1] = new ReentrantReadWriteLock();
        this.buildNext(1);
    }

    public T next() {
        Iterator<T> c = this.current();
        if (c == null || !c.hasNext()) {
            this.switchIter();
            if (this.cp[this.current] != null) {
                waitcurrent();
            }
            c = this.current();
            this.buildNext(1 - this.current);
        }
        return c != null && c.hasNext() ? c.next() : null;
    }

    private void waitcurrent() {
        CompletableFuture currentCp = this.cp[this.current];
        if (currentCp != null) {
            try {
                this.cp[this.current] = null;
                currentCp.get();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private  Iterator<T> current() {
        try {
            this.lock[ this.current ].readLock().lock();
            return this.currents[ this.current ];
        }
        finally {
            this.lock[ this.current ].readLock().unlock();
        }
    }

    private  void putIter(int indice, Iterator<T> iter) {
        try {
            this.lock[ indice ].writeLock().lock();
            this.currents[ indice ] = iter;
        }
        finally {
            this.lock[ indice ].writeLock().unlock();
        }
    }

    private void switchIter() {
        try {
            this.lock[ 0 ].writeLock().lock();
            this.lock[ 1 ].writeLock().lock();
            this.current = 1 - this.current; // switch
        }
        finally {
            this.lock[ 0 ].writeLock().unlock();
            this.lock[ 1 ].writeLock().unlock();
        }
    }

    private void buildNext(int numtobuild) {
        final Lock wr = this.lock[ numtobuild ].writeLock();

        CompletableFuture cpf = CompletableFuture.supplyAsync(() -> {
                wr.lock(); return this.supplier.get();
            },
                exe)
                    .thenAccept( x -> this.putIter(numtobuild, x))
                    .thenAccept( (Void x) -> { wr.unlock(); });

        this.cp[ numtobuild ] = cpf;
    }

    private Callable<Iterator<T>> call() {
        return () -> this.supplier.get();
    }

}
