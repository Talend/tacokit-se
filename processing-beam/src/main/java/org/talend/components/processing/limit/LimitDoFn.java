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
package org.talend.components.processing.limit;

import org.apache.avro.generic.IndexedRecord;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Limit {@link DoFn} used to limit the number of records processed from the source
 * {@link org.apache.beam.sdk.values.PCollection}. At the time of writing, this {@link DoFn} uses a static counter
 * defined at the {@link Limit} {@link org.apache.beam.sdk.transforms.PTransform} level, which makes it unusable
 * in a distributed mode. Consequently, it can be used only with local runners.
 */
public class LimitDoFn extends DoFn<IndexedRecord, IndexedRecord> {

    private LimitConfiguration configuration = null;

    @ProcessElement
    public void processElement(ProcessContext context) {
        if (Limit.counter.getAndIncrement() < configuration.getLimit()) {
            context.output(context.element());
        }
    }

    public LimitDoFn withConfiguration(LimitConfiguration configuration) {
        this.configuration = configuration;
        return this;
    }
}