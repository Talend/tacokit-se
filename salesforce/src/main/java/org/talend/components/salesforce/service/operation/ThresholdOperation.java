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
}
