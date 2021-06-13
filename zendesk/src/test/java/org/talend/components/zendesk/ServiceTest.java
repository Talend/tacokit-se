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
package org.talend.components.zendesk;

import static org.mockserver.integration.ClientAndServer.startClientAndServer;
import static org.mockserver.model.HttpRequest.request;
import static org.mockserver.model.HttpResponse.response;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockserver.integration.ClientAndServer;
import org.mockserver.model.MediaType;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;

public class ServiceTest extends ZendeskTestBase {

    @Test
    public void test() throws IOException {
        URL resource = this.getClass().getResource("/user.json");
        String User = FileUtils.readFileToString(new File(resource.getFile()), "UTF-8");
        mockServer.when(request().withMethod("GET").withPath("/api/v2/users/me.json"))
                .respond(response().withContentType(MediaType.APPLICATION_JSON).withBody(User));

        HealthCheckStatus healthCheckStatus = zendeskService.validateBasicConnection(datastore);
        Assert.assertEquals(healthCheckStatus.getComment(), HealthCheckStatus.Status.OK, healthCheckStatus.getStatus());
    }

}
