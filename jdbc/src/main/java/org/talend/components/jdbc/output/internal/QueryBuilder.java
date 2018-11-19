package org.talend.components.jdbc.output.internal;

import java.util.List;
import java.util.Map;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

public interface QueryBuilder {

    String buildQuery(List<Record> records);

    boolean validateQueryParam(Record record);

    Map<Integer, Schema.Entry> getQueryParams();
}
