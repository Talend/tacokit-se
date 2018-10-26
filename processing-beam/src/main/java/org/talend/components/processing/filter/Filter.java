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
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Output;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.api.processor.Processor;

import javax.json.JsonObject;

import static org.talend.sdk.component.api.component.Icon.IconType.FILTER_ROW;

@Version
@Icon(FILTER_ROW)
@Processor(name = "Filter")
@Documentation("This component filters the input with some logical rules.")
public class Filter extends PTransform<PCollection<IndexedRecord>, PCollectionTuple> {

    public final static String FLOW_CONNECTOR = "__default__";

    public final static String REJECT_CONNECTOR = "reject";

    private final static TupleTag<IndexedRecord> flowOutput = new TupleTag<IndexedRecord>(FLOW_CONNECTOR) {
    };

    final static TupleTag<IndexedRecord> rejectOutput = new TupleTag<IndexedRecord>(REJECT_CONNECTOR) {
    };

    private FilterConfiguration configuration;

    public Filter(@Option("configuration") final FilterConfiguration configuration) {
        this.configuration = configuration;
    }

    @ElementListener
    public void onElement(final JsonObject element, @Output final OutputEmitter<JsonObject> output,
            @Output(REJECT_CONNECTOR) final OutputEmitter<JsonObject> reject) {
        // no-op
    }

    @Override
    public PCollectionTuple expand(PCollection<IndexedRecord> input) {
        return input.apply(ParDo.of(new FilterDoFn(configuration)).withOutputTags(flowOutput, TupleTagList.of(rejectOutput)));
    }

    // TODO: Currently the component always sends data on output and reject links, even if they are not
    // used in the pipeline. it should change.
    /*
     * public void build(BeamJobContext ctx) {
     * String mainLink = ctx.getLinkNameByPortName("input_" + FLOW_CONNECTOR);
     * if (!StringUtils.isEmpty(mainLink)) {
     * PCollection<IndexedRecord> mainPCollection = ctx.getPCollectionByLinkName(mainLink);
     * if (mainPCollection != null) {
     * String flowLink = ctx.getLinkNameByPortName("output_" + FLOW_CONNECTOR);
     * String rejectLink = ctx.getLinkNameByPortName("output_" + REJECT_CONNECTOR);
     * 
     * boolean hasFlow = StringUtils.isNotEmpty(flowLink);
     * boolean hasReject = StringUtils.isNotEmpty(rejectLink);
     * 
     * if (hasFlow && hasReject) {
     * // If both of the outputs are present, the DoFn must be used.
     * PCollectionTuple outputTuples = mainPCollection.apply(ctx.getPTransformName(),
     * 
     * ctx.putPCollectionByLinkName(flowLink, outputTuples.get(flowOutput));
     * ctx.putPCollectionByLinkName(rejectLink, outputTuples.get(rejectOutput));
     * } else if (hasFlow || hasReject) {
     * // If only one of the outputs is present, the predicate can be used for efficiency.
     * FilterPredicate predicate = hasFlow //
     * ? new FilterPredicate(configuration) //
     * : new FilterPredicate.Negate(configuration);
     * PCollection<IndexedRecord> output = mainPCollection.apply(ctx.getPTransformName(),
     * org.apache.beam.sdk.transforms.Filter.by(predicate));
     * ctx.putPCollectionByLinkName(hasFlow ? flowLink : rejectLink, output);
     * } else {
     * // If neither are specified, then don't do anything. This component could have been cut from the pipeline.
     * }
     * }
     * }
     * }
     */
}
