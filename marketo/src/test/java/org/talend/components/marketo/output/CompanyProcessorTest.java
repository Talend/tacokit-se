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
package org.talend.components.marketo.output;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import javax.json.JsonObject;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Ignore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.DeleteBy;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.SyncMethod;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit.beam.Data;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.beam.TalendFn;
import org.talend.sdk.component.runtime.output.Processor;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class CompanyProcessorTest extends MarketoProcessorBaseTest {

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        outputDataSet.setEntity(MarketoEntity.Company);
        outputDataSet.setDedupeBy("dedupeFields");
        //
        data = jsonFactory.createObjectBuilder().add("externalCompanyId", "google666").build();
        dataNotExist = jsonFactory.createObjectBuilder().add("externalCompanyId", "UnbelievableGoogleXYZ").build();
        // we create a record
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);
        initProcessor();
        processor.map(data, main -> {
        }, reject -> fail("Should not fail as createOrUpdate"));
    }

    private void initProcessor() {
        processor = new MarketoProcessor(outputDataSet, service, tools);
        processor.init();
    }

    @AfterEach
    void tearDown() {
        outputDataSet.setAction(OutputAction.delete);
        outputDataSet.setDeleteBy(DeleteBy.dedupeFields);
        initProcessor();
        processor.map(data, main -> {
        }, reject -> {
        });
    }

    @Test
    void testSyncCompanies() {
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);
        initProcessor();
        processor.map(data, main -> assertEquals(0, main.getInt("seq")), reject -> fail(FAIL_REJECT));
    }

    @Test
    void testDeleteCompanies() {
        outputDataSet.setAction(OutputAction.delete);
        outputDataSet.setDeleteBy(DeleteBy.dedupeFields);
        initProcessor();
        processor.map(data, main -> assertEquals(0, main.getInt("seq")), reject -> fail(FAIL_REJECT));
    }

    @Test
    void testDeleteCompaniesFail() {
        outputDataSet.setAction(OutputAction.delete);
        outputDataSet.setDeleteBy(DeleteBy.dedupeFields);
        initProcessor();
        processor.map(dataNotExist, main -> fail(FAIL_MAIN),
                reject -> assertEquals("1013", reject.getJsonArray("reasons").get(0).asJsonObject().getString("code")));
    }

    @Test
    void testSyncCompaniesFail() {
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.updateOnly);
        initProcessor();
        processor.map(dataNotExist, main -> fail(FAIL_MAIN),
                reject -> assertEquals("1013", reject.getJsonArray("reasons").get(0).asJsonObject().getString("code")));
    }

    @Test
    public void testSyncCompaniesProcessor() {
        outputDataSet.setSyncMethod(SyncMethod.updateOnly);
        final Processor processor = component.createProcessor(MarketoProcessor.class, outputDataSet);
        final SimpleComponentRule.Outputs outputs = component.collect(processor,
                new JoinInputFactory().withInput("__default__", asList(data, data, dataNotExist)));
        assertEquals(2, outputs.size());
        List<JsonObject> mains = outputs.get(JsonObject.class, "__default__");
        assertEquals(2, mains.size());
        assertEquals(0, mains.get(0).getInt("seq"));
        List<JsonObject> rejects = outputs.get(JsonObject.class, "rejected");
        assertEquals(1, rejects.size());
        assertEquals(0, rejects.get(0).getInt("seq"));
        assertEquals("1013", rejects.get(0).getJsonArray("reasons").get(0).asJsonObject().getString("code"));
    }

    @Ignore
    @Test
    void processor() {
        // Setup your component configuration for the test here
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);

        Pipeline pipeline = Pipeline.create();

        // We create the component processor instance using the configuration filled above
        final Processor processor = component.createProcessor(MarketoProcessor.class, outputDataSet);
        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", asList(data
        // TODO - list of your input data for this branch.Instances of OutputTableDefaultInput.class
        ));
        // Convert it to a beam "source"
        final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));
        // add our processor right after to see each data as configured previously
        final PCollection<Map<String, Record>> outputs = inputs.apply(TalendFn.asFn(processor))
                .apply(Data.map(processor.plugin(), Record.class));
        PAssert.that(outputs).satisfies((SerializableFunction<Iterable<Map<String, Record>>, Void>) input -> {
            final List<Map<String, Record>> result = StreamSupport.stream(input.spliterator(), false).collect(toList());
            // TODO - test the result here
            LOG.warn("[processor] results: {}", result);
            return null;
        });
        // run the pipeline and ensure the execution was successful
        assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }
}
