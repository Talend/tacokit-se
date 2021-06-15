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
package org.talend.components.zendesk.source;

import static org.junit.Assert.assertEquals;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.IOException;

import org.junit.ClassRule;
import org.junit.Test;
import org.mockserver.model.MediaType;
import org.talend.components.zendesk.ZendeskTestBase;
import org.talend.components.zendesk.common.SelectionType;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.junit.SimpleComponentRule;
import org.talend.sdk.component.runtime.input.Mapper;

public class ZendeskInputMapperTest extends ZendeskTestBase {

    @ClassRule
    public static final SimpleComponentRule COMPONENT_FACTORY = new SimpleComponentRule("org.talend.components");

    @Test
    public void TicketInput() throws IOException {
        mockServer.when(request().withMethod("GET").withPath("/api/v2/tickets.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withBody(getResourceString("/tickets.json")));

        // Source configuration
        // Setup your component configuration for the test here
        final ZendeskInputMapperConfiguration configuration = new ZendeskInputMapperConfiguration();
        configuration.setDataset(dataset);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ZendeskInputMapper.class, configuration);

        // Collect the source as a list
        assertEquals(16, COMPONENT_FACTORY.collectAsList(Record.class, mapper).size());
    }

    @Test
    public void RequestInput() throws IOException {
        mockServer.when(request().withMethod("GET").withPath("/api/v2/requests.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withBody(getResourceString("/requests.json")));
        // Source configuration
        // Setup your component configuration for the test here
        final ZendeskInputMapperConfiguration configuration = new ZendeskInputMapperConfiguration();
        dataset.setSelectionType(SelectionType.REQUESTS);
        configuration.setDataset(dataset);

        // We create the component mapper instance using the configuration filled above
        final Mapper mapper = COMPONENT_FACTORY.createMapper(ZendeskInputMapper.class, configuration);

        // Collect the source as a list
        assertEquals(2, COMPONENT_FACTORY.collectAsList(String.class, mapper).size());
    }

}