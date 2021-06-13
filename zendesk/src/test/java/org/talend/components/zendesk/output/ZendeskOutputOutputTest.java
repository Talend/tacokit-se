/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.zendesk.output;

import static org.junit.Assert.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.IOException;

import org.junit.Test;
import org.mockserver.model.MediaType;
import org.mockserver.verify.VerificationTimes;
import org.talend.components.zendesk.ZendeskTestBase;
import org.talend.sdk.component.junit.JoinInputFactory;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.output.Processor;

public class ZendeskOutputOutputTest extends ZendeskTestBase {

    @Test
    public void map() throws IOException {
        mockServer.when(request().withMethod("POST").withPath("/api/v2/tickets/*.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withBody("{}"));

        // Output configuration
        // Setup your component configuration for the test here
        final ZendeskOutputConfiguration configuration = new ZendeskOutputConfiguration();
        configuration.setDataset(dataset);
        configuration.setUseBatch(false);
        /*
         * .setDataset()
         * .setConfiguration2()
         */;

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(ZendeskOutput.class, configuration);

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", createData(2));

        // Run the flow and get the outputs
        final SimpleComponentRule.Outputs outputs = COMPONENT_FACTORY.collect(processor, joinInputFactory);
        System.out.println(outputs);

        mockServer.verify(request().withPath("/api/v2/tickets/1.json").withMethod("PUT"), VerificationTimes.exactly(1));
        mockServer.verify(request().withPath("/api/v2/tickets/2.json").withMethod("PUT"), VerificationTimes.exactly(1));

    }

    @Test
    public void InsertBatch() throws IOException {
        mockServer.when(request().withMethod("PUT").withPath("/api/v2/tickets/update_many.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withStatusCode(200)
                        .withBody(getResourceString("/create_many.json")));
        mockServer.when(request().withMethod("POST").withPath("/api/v2/tickets/create_many.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withStatusCode(200)
                        .withBody(getResourceString("/create_many.json")));
        mockServer.when(request().withMethod("GET").withPath("/api/v2/job_statuses/4aba648d81905e6492679833541685a7.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withStatusCode(200)
                        .withBody(getResourceString("/jobStatus.json")));

        // Output configuration
        // Setup your component configuration for the test here
        final ZendeskOutputConfiguration configuration = new ZendeskOutputConfiguration();
        configuration.setDataset(dataset);
        configuration.setUseBatch(true);
        /*
         * .setDataset()
         * .setConfiguration2()
         */;

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(ZendeskOutput.class, configuration);

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", createData(2));

        // Run the flow and get the outputs
        final SimpleComponentRule.Outputs outputs = COMPONENT_FACTORY.collect(processor, joinInputFactory);
        System.out.println(outputs);

        mockServer.verify(request().withPath("/api/v2/job_statuses/4aba648d81905e6492679833541685a7.json").withMethod("GET"),
                VerificationTimes.exactly(1));
        mockServer.verify(request().withPath("/api/v2/tickets/update_many.json").withMethod("PUT"), VerificationTimes.exactly(1));

    }

    @Test
    public void DeleteBatch() throws IOException {
        mockServer.when(request().withMethod("DELETE").withPath("/api/v2/tickets/destroy_many.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withBody("{}"));

        // Output configuration
        // Setup your component configuration for the test here
        final ZendeskOutputConfiguration configuration = new ZendeskOutputConfiguration();
        configuration.setDataset(dataset);
        configuration.setDataAction(DataAction.DELETE);
        configuration.setUseBatch(false);
        /*
         * .setDataset()
         * .setConfiguration2()
         */;

        // We create the component processor instance using the configuration filled above
        final Processor processor = COMPONENT_FACTORY.createProcessor(ZendeskOutput.class, configuration);

        // The join input factory construct inputs test data for every input branch you have defined for this component
        // Make sure to fil in some test data for the branches you want to test
        // You can also remove the branches that you don't need from the factory below
        final JoinInputFactory joinInputFactory = new JoinInputFactory().withInput("__default__", createData(2));

        // Run the flow and get the outputs
        final SimpleComponentRule.Outputs outputs = COMPONENT_FACTORY.collect(processor, joinInputFactory);

        mockServer.verify(request().withPath("/api/v2/tickets/destroy_many.json").withMethod("DELETE"),
                VerificationTimes.exactly(1));

    }

}