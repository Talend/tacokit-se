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
package org.talend.components.common.stream.api.output;

import java.io.IOException;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.sdk.component.api.record.Record;

/**
 * Write record to a destination.
 */
public interface RecordWriter extends AutoCloseable {

    default void init(ContentFormat config) throws IOException {
    }

    default void end() throws IOException {
        this.flush();
        this.close();
    }

    /**
     * add record to writer.
     * 
     * @param record : input record.
     */
    void add(Record record) throws IOException;

    /**
     * add records to writer.
     * 
     * @param records : input record.
     */
    default void add(Iterable<Record> records) throws IOException {
        for (Record record : records) {
            this.add(record);
        }
    }

    void flush() throws IOException;

    void close() throws IOException;
}
