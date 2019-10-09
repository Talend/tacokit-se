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
package org.talend.components.rest.configuration;

import lombok.Data;
import org.talend.components.rest.configuration.auth.Authentication;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.action.Updatable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Version(1)
@Data
@DataSet("Dataset")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "resource" }), @GridLayout.Row({ "methodType" }),
        @GridLayout.Row({ "authentication" }), @GridLayout.Row({ "hasPathParams" }), @GridLayout.Row({ "pathParams" }),
        @GridLayout.Row({ "hasHeaders" }), @GridLayout.Row({ "headers" }), @GridLayout.Row({ "hasQueryParams" }),
        @GridLayout.Row({ "queryParams" }), @GridLayout.Row({ "body" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "redirect", "maxRedirect", "force_302_redirect" }),
        @GridLayout.Row({ "connectionTimeout" }), @GridLayout.Row({ "readTimeout" }) })
@Documentation("Define the resource and authentication")
public class Dataset implements Serializable {

    @Option
    @Documentation("Identification of the REST API")
    private Datastore datastore;

    @Option
    @Required
    @DefaultValue("GET")
    @Documentation("Action on the resource")
    private HttpMethod methodType;

    @Option
    @Required
    @Documentation("End of url to complete base url of the datastore")
    // @Suggestable(value = "getPaths", parameters = { "../datastore" })
    private String resource;

    @Option
    @Required
    @Documentation("")
    private Authentication authentication;

    @Min(0)
    @Option
    @Required
    @Documentation("")
    @DefaultValue("500")
    private Integer connectionTimeout;

    @Min(0)
    @Option
    @Required
    @Documentation("")
    @DefaultValue("500")
    private Integer readTimeout;

    @Option
    @Documentation("")
    @DefaultValue("false")
    private Boolean redirect = false;

    @Option
    @Documentation("")
    @DefaultValue("1")
    @ActiveIf(target = "redirect", value = "true")
    @Min(-1)
    private Integer maxRedirect = 1;

    @Option
    @Documentation("")
    @DefaultValue("false")
    @ActiveIf(target = "redirect", value = "true")
    private Boolean force_302_redirect;

    @Option
    @Documentation("Http request contains path parameters")
    private Boolean hasPathParams = false;

    @Option
    @ActiveIf(target = "hasPathParams", value = "true")
    @Documentation("Http path parameters")
    private List<Param> pathParams = new ArrayList<>();

    @Option
    @Documentation("Http request contains headers")
    private Boolean hasHeaders = false;

    @Option
    @ActiveIf(target = "hasHeaders", value = "true")
    @Documentation("Http request headers")
    private List<Param> headers = new ArrayList<>();

    @Option
    @Documentation("Http request contains query params")
    private Boolean hasQueryParams = false;

    @Option
    @ActiveIf(target = "hasQueryParams", value = "true")
    @Documentation("Http request query params")
    private List<Param> queryParams = new ArrayList<>();

    @Option
    @ActiveIf(target = "methodType", value = { "POST", "PUT", "PATCH", "DELETE", "OPTIONS" })
    @Documentation("")
    private RequestBody body;

}
