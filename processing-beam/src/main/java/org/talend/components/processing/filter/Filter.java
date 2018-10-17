// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
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
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.adapter.beam.BeamJobBuilder;
import org.talend.components.adapter.beam.BeamJobContext;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import java.io.Serializable;

import static org.talend.sdk.component.api.component.Icon.IconType.FILTER_ROW;

@Version
@Icon(FILTER_ROW)
@Processor(name = "Filter")
@Documentation("This component filters the input with some logical rules.")
public class Filter implements BeamJobBuilder, Serializable {

    private final static TupleTag<IndexedRecord> flowOutput = new TupleTag<IndexedRecord>() {
    };

    final static TupleTag<IndexedRecord> rejectOutput = new TupleTag<IndexedRecord>() {
    };

    private FilterConfiguration configuration;

    public Filter(@Option("configuration") final FilterConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output,
            @Output("reject") final OutputEmitter<JsonObject> reject) {
    }

    @Override
    public void build(BeamJobContext ctx) {
        String mainLink = ctx.getLinkNameByPortName("input_" + configuration.MAIN_CONNECTOR);
        if (!StringUtils.isEmpty(mainLink)) {
            PCollection<IndexedRecord> mainPCollection = ctx.getPCollectionByLinkName(mainLink);
            if (mainPCollection != null) {
                String flowLink = ctx.getLinkNameByPortName("output_" + configuration.FLOW_CONNECTOR);
                String rejectLink = ctx.getLinkNameByPortName("output_" + configuration.REJECT_CONNECTOR);

                boolean hasFlow = StringUtils.isNotEmpty(flowLink);
                boolean hasReject = StringUtils.isNotEmpty(rejectLink);

                if (hasFlow && hasReject) {
                    // If both of the outputs are present, the DoFn must be used.
                    PCollectionTuple outputTuples = mainPCollection.apply(ctx.getPTransformName(),
                            ParDo.of(new FilterDoFn(configuration)).withOutputTags(flowOutput, TupleTagList.of(rejectOutput)));
                    ctx.putPCollectionByLinkName(flowLink, outputTuples.get(flowOutput));
                    ctx.putPCollectionByLinkName(rejectLink, outputTuples.get(rejectOutput));
                } else if (hasFlow || hasReject) {
                    // If only one of the outputs is present, the predicate can be used for efficiency.
                    FilterPredicate predicate = hasFlow //
                            ? new FilterPredicate(configuration) //
                            : new FilterPredicate.Negate(configuration);
                    PCollection<IndexedRecord> output = mainPCollection.apply(ctx.getPTransformName(),
                            org.apache.beam.sdk.transforms.Filter.by(predicate));
                    ctx.putPCollectionByLinkName(hasFlow ? flowLink : rejectLink, output);
                } else {
                    // If neither are specified, then don't do anything. This component could have been cut from the pipeline.
                }
            }
        }
    }
}
