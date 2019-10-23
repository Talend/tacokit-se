/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.workday.input;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.talend.components.workday.service.ConfigHelper;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.junit5.ComponentExtension;
import org.talend.sdk.component.junit5.WithComponents;
import org.talend.sdk.component.runtime.manager.chain.Job;

import javax.json.JsonObject;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

@ExtendWith(ComponentExtension.class)
@WithComponents("org.talend.components.workday")
class WorkdayProducerTest {

    @Test
    public void producer() {
        Properties props = ConfigHelper.workdayProps();
        System.setProperty("talend.beam.job.targetParallelism", "1"); // our code creates one hz lite instance per thread
        Job.components()
                .component("source",
                        "Workday://Input?" + "configuration.dataSet.service=common/v1/workers&"
                                + "configuration.dataSet.datastore.authEndpoint=" + props.getProperty("authendpoint") + "&"
                                + "configuration.dataSet.datastore.clientId=" + props.getProperty("clientId") + "&"
                                + "configuration.dataSet.datastore.clientSecret=" + props.getProperty("clientSecret") + "&"
                                + "configuration.dataSet.datastore.endpoint=" + props.getProperty("endpoint") + "&"
                                + "configuration.dataSet.datastore.tenantAlias=" + props.getProperty("tenant"))
                .component("output", "WorkdayTest://collector").connections().from("source").to("output").build().run();
    }

    @Processor(family = "WorkdayTest", name = "collector")
    public static class TesTOutput implements Serializable {

        private static final long serialVersionUID = -989062340811827429L;

        static final List<JsonObject> OBJECTS = new ArrayList<>();

        @ElementListener
        public void onNext(final JsonObject object) {
            synchronized (OBJECTS) {
                OBJECTS.add(object);
            }
        }
    }
}