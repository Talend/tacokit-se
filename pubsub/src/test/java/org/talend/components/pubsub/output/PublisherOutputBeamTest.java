package org.talend.components.pubsub.output;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.talend.sdk.component.junit.SimpleComponentRule;

public class PublisherOutputBeamTest {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components.pubsub");

    // @Rule
    // public transient final TestPipeline pipeline = TestPipeline.create();

    @Test
    @Ignore("You need to complete this test with your own data and assertions")
    public void processor() {
        // Output configuration
        // Setup your component configuration for the test here
        // final PublisherOutputConfiguration configuration = new PublisherOutputConfiguration();
        // // We create the component processor instance using the configuration filled above
        // final Processor processor = COMPONENT_FACTORY.createProcessor(PublisherOutput.class, configuration);
        // // The join input factory construct inputs test data for every input branch you have defined for this component
        // // Make sure to fil in some test data for the branches you want to test
        // // You can also remove the branches that you don't need from the factory below
        // final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__",
        // asList(/* TODO - list of your input data for this branch. Instances of PublisherDefaultInput.class */));
        // // Convert it to a beam "source"
        // final PCollection<Record> inputs = pipeline.apply(Data.of(processor.plugin(), joinInputFactory.asInputRecords()));
        // // add our processor right after to see each data as configured previously
        // inputs.apply(TalendFn.asFn(processor)).apply(Data.map(processor.plugin(), Record.class));
        // // run the pipeline and ensure the execution was successful
        // assertEquals(PipelineResult.State.DONE, pipeline.run().waitUntilFinish());
    }
}
