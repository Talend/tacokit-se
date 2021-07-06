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
package org.talend.components.salesforce.service.operation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.talend.sdk.component.api.record.Record;

public class ThresholdOperation {

    private final RecordsOperation operation;

    private final List<Record> records;

    private final int commitLevel;

    public ThresholdOperation(RecordsOperation operation, int commitLevel) {
        this.operation = operation;
        this.commitLevel = commitLevel;
        this.records = new ArrayList<>(this.commitLevel);
    }

    public synchronized List<Result> execute(Record record) throws IOException {
        this.records.add(record);
        if (this.records.size() >= this.commitLevel) {
            return this.terminate();
        }
        return null;
    }

    public synchronized List<Result> terminate() throws IOException {
        final List<Result> results = this.operation.execute(this.records);
        this.records.clear();
        return results;
    }

    public String name() {
        return this.operation.name();
    }
}
