package org.talend.components.workday.input;

import org.junit.ClassRule;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.junit5.ComponentExtension;
import org.talend.sdk.component.runtime.manager.chain.Job;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComponentExtension.class)
class WorkdayProducerTest {

    @Test
    public void run() {
        System.setProperty("talend.beam.job.targetParallelism", "1"); // our code creates one hz lite instance per thread
        Job.components()
                .component("source", "Workday://Input")
                .component("output", "Hazelcast://Output?" +
                        "configuration.mapName=" + HazelcastProcessorTest.class.getSimpleName() + "&" +
                        "configuration.keyAttribute=key&" +
                        "configuration.valueAttribute=value")
                .connections()
                .from("source").to("output")
                .build()
                .run();
    }

}