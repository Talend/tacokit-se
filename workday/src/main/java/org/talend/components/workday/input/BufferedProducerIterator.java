package org.talend.components.workday.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.workday.WorkdayException;

import java.util.Iterator;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Help to get a full iterator from a 'per page' getter.
 * Many REST services that provide array work with per page system
 * example : get?page=1 ... page=20 ...
 * This class simplify client interface for this kind of service by exposing simple iterator.
 * while itering on a page, it load the next, and so on.
 * @param <T>
 */
@Slf4j
public class BufferedProducerIterator<T> {

    @FunctionalInterface
    public static interface PageGetter<T> {
        Iterator<T> find(int pageNumber);
    }

    private final ExecutorService exe = Executors.newFixedThreadPool(2);

    /** result pages */
    private PageRetriever<T>[] retrivers = new PageRetriever[2];

    /** current page */
    private int currentRetriver = 0;

    /** next page to retrieve */
    private final AtomicInteger nextPage;

    private final PageGetter<T> getter;

    public BufferedProducerIterator(PageGetter<T> getter) {
        this.retrivers[0] = new PageRetriever<>();
        this.retrivers[1] = new PageRetriever<>();
        this.getter = getter;

        this.retrivers[0].buildNext(exe, getter, 0);
        this.retrivers[1].buildNext(exe, getter, 1);
        nextPage = new AtomicInteger(2);
    }

    public T next() {
        Iterator<T> c = this.getCurrent().getPageIterator();
        if (c == null) {
            return null;
        }
        if (!c.hasNext()) {
            this.getCurrent().buildNext(exe, getter, nextPage.getAndIncrement());
            this.currentRetriver = 1 - this.currentRetriver;
            return this.next();
        }
        return c.next();
    }

    private PageRetriever<T> getCurrent() {
        return this.retrivers[ this.currentRetriver ];
    }


    private static class PageRetriever<T> {

        private Iterator<T> currentPage = null;

        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

        private CompletableFuture<Iterator<T>> futurePage = null;

        public void buildNext(Executor exe, PageGetter<T> getter, int pageNumber) {
            final Lock wr = this.lock.writeLock();
            this.currentPage = null;
            this.futurePage = CompletableFuture.supplyAsync(() -> {
                        wr.lock(); return getter.find(pageNumber);
                    },
                    exe)
                    .whenComplete( (input, exception) -> { wr.unlock(); });
        }

        public Iterator<T> getPageIterator() {
            this.lock.readLock().lock();
            try {
                if (this.currentPage == null && this.futurePage != null) {
                    this.currentPage = this.futurePage.get();
                }
                return this.currentPage;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("execution exception try to get workday page", e);
                throw new WorkdayException("Error with workday");
            }
            catch (ExecutionException e) {
                log.error("execution exception try to get workday page", e);
                throw new WorkdayException("Error with workday");
            }
            finally {
                this.lock.readLock().unlock();
            }
        }

    }

}
