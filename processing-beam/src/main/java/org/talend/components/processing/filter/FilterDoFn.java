// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.processing.filter;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * This Beam function is exclusively used when a FilterRow requires both a main and reject output. If only one or the other is
 * required, the {@link FilterPredicate} should be used directly to avoid
 * creating extra outputs.
 */
public class FilterDoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private final FilterPredicate predicate;

    public FilterDoFn(FilterConfiguration configuration) {
        this.predicate = new FilterPredicate(configuration);
    }

    @ProcessElement
    public void processElement(ProcessContext context) {
        IndexedRecord record = context.element();
        if (predicate.apply(record)) {
            context.output(record);
        } else {
            context.output(Filter.rejectOutput, record);
        }
    }
}
