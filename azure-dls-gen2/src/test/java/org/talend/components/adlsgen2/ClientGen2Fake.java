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
package org.talend.components.adlsgen2;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import javax.json.JsonObject;

import org.talend.components.adlsgen2.service.AdlsGen2APIClient;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.injector.Injector;
import org.talend.sdk.component.container.Container;
import org.talend.sdk.component.runtime.manager.ComponentManager;
import org.talend.sdk.component.runtime.manager.ComponentManager.AllServices;

public class ClientGen2Fake implements AdlsGen2APIClient {

    public static void inject(final ComponentManager manager, final ClientGen2Fake client) {
        final Container container = manager.findPlugin("classes").orElse(null);

        // final ClientGen2Fake client = new ClientGen2Fake(new ClientGen2Fake.FakeResponse<>(filesystems, 200));
        final AllServices allServices = container.get(AllServices.class);
        allServices.getServices().put(AdlsGen2APIClient.class, client);
        final AdlsGen2Service gen2Service = (AdlsGen2Service) allServices.getServices().get(AdlsGen2Service.class);
        final Injector injector = Injector.class.cast(allServices.getServices().get(Injector.class));
        injector.inject(gen2Service);
    }

    private final Response<JsonObject> preparedResponse;

    private final Response<InputStream> pathReadResponse;

    public ClientGen2Fake(Response<JsonObject> preparedResponse) {
        this(preparedResponse, null);
    }

    public ClientGen2Fake(final Response<JsonObject> preparedResponse, final Response<InputStream> pathReadResponse) {
        this.preparedResponse = preparedResponse;
        this.pathReadResponse = pathReadResponse;
    }

    @Override
    public Response<JsonObject> filesystemList(Map<String, String> headers, Map<String, String> sas, String resource,
            Integer timeout) {
        return this.preparedResponse;
    }

    @Override
    public Response<JsonObject> filesystemList(Map<String, String> headers, Boolean prefix, String continuation,
            Integer maxResults, Integer timeout) {
        return this.preparedResponse;
    }

    @Override
    public Response<JsonObject> pathList(Map<String, String> headers, String filesystem, Map<String, String> sas,
            String directory, String resource, Boolean recursive, String continuation, Integer maxResults, Integer timeout) {
        return this.preparedResponse;
    }

    @Override
    public Response<InputStream> pathRead(Map<String, String> headers, String filesystem, String path, Integer timeout,
            Map<String, String> sas) {
        return this.pathReadResponse;
    }

    @Override
    public Response<JsonObject> pathGetProperties(Map<String, String> headers, String filesystem, String path, Integer timeout,
            Map<String, String> sas) {
        return this.preparedResponse;
    }

    @Override
    public Response<JsonObject> pathCreate(Map<String, String> headers, String filesystem, String path, String resource,
            Integer timeout, Map<String, String> sas, String emptyPayload) {
        return this.preparedResponse;
    }

    @Override
    public Response<JsonObject> pathUpdate(Map<String, String> headers, String filesystem, String path, String action,
            long position, Integer timeout, Map<String, String> sas, byte[] payload) {
        return this.preparedResponse;
    }

    @Override
    public void base(String base) {

    }
}
