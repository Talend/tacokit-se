package org.talend.components.pubsub.source;

import java.io.Serializable;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.talend.sdk.component.junit.SimpleComponentRule;

public class SubscriberMapperBeamTest implements Serializable {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.pubsub");

    @Rule
    // public transient final TestPipeline pipeline = TestPipeline.create();

    @Test
    @Ignore("You need to complete this test with your own data and assertions")
    public void produce() {
        // // Setup your component configuration for the test here
        // final SubscriberMapperConfiguration configuration = new SubscriberMapperConfiguration();
        //
        // // We create the component mapper instance using the configuration filled above
        // final Mapper mapper = COMPONENT_FACTORY.createMapper(SubscriberMapper.class, configuration);
        //
        // // create a pipeline starting with the mapper
        // final PCollection<Record> out = pipeline.apply(TalendIO.read(mapper));
        //
        // // then append some assertions to the output of the mapper,
        // // PAssert is a beam utility to validate part of the pipeline
        // PAssert.that(out).containsInAnyOrder(/* TODO - give the expected data */);
        //
        // // finally run the pipeline and ensure it was successful - i.e. data were validated
        // assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
        // waitUntilFinish
    }
}
