package org.talend.components.salesforce.service.operation;

import java.io.IOException;
import java.util.List;

import org.talend.sdk.component.api.record.Record;

public interface RecordsOperation {

    List<Result> execute(List<Record> records) throws IOException;
}
