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
package org.talend.components.common.stream.input.parquet;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.parquet.hadoop.ParquetReader;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParquetIterator<T> implements Iterator<T> {

    private final ParquetReader<T> reader;

    private T currentElement;

    public ParquetIterator(ParquetReader<T> reader) {
        this.reader = reader;
        this.currentElement = this.findNext();
    }

    @Override
    public boolean hasNext() {
        return this.currentElement != null;
    }

    @Override
    public T next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("No more element in this parquet input");
        }
        final T nextElement = this.currentElement;
        this.currentElement = this.findNext();
        return nextElement;
    }

    private T findNext() {
        try {
            final T next = this.reader.read();
            if (next == null) {
                this.reader.close();
            }
            return next;
        } catch (IOException ex) {
            log.error("Can't read next record : " + ex.getMessage(), ex);
            throw new UncheckedIOException("Can't read next record : " + ex.getMessage(), ex);
        }
    }
}
