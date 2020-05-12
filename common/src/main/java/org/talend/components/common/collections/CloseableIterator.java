/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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

import java.util.Iterator;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class CloseableIterator<T> implements Iterator<T>, AutoCloseable {

    private final Iterator<T> wrapped;

    private final AutoCloseable closeFunction;

    private boolean isClosed = false;

    public CloseableIterator(Iterator<T> wrapped, AutoCloseable closeFunction) {
        this.wrapped = wrapped;
        this.closeFunction = closeFunction;
    }

    @Override
    public boolean hasNext() {
        final boolean hasNext = this.wrapped.hasNext();
        if (!hasNext) {
            this.close();
        }
        return hasNext;
    }

    @Override
    public T next() {
        return this.wrapped.next();
    }

    @Override
    public synchronized void close() {
        if (this.isClosed) {
            return; // run once.
        }
        try {
            this.closeFunction.close();
        } catch (Exception ex) {
            log.error("Close error : " + ex.getMessage(), ex);
            throw new RuntimeException("Close error : " + ex.getMessage(), ex);
        } finally {
            this.isClosed = true;
        }
    }
}