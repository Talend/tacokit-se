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
package org.talend.components.rest.configuration;

import lombok.Data;
import org.talend.components.extension.polling.api.PollableDuplicateDataset;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Data
@DataSet("Dataset")
@PollableDuplicateDataset
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "resource" }), @GridLayout.Row({ "methodType" }),
        @GridLayout.Row({ "format" }), @GridLayout.Row({ "hasHeaders" }), @GridLayout.Row({ "headers" }),
        @GridLayout.Row({ "hasQueryParams" }), @GridLayout.Row({ "queryParams" }), @GridLayout.Row({ "hasPathParams" }),
        @GridLayout.Row({ "pathParams" }), @GridLayout.Row({ "hasBody" }), @GridLayout.Row({ "body" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "datastore" }),
        @GridLayout.Row({ "completePayload" }), @GridLayout.Row({ "maxRedirect" }), @GridLayout.Row({ "only_same_host" }),
        @GridLayout.Row({ "force_302_redirect" }), @GridLayout.Row({ "jsonForceDouble" }) })
@Documentation("Dataset configuration.")
public class Dataset implements Serializable {

    @Option
    @Documentation("Identification of the REST API.")
    private Datastore datastore;

    @Option
    @Required
    @DefaultValue("GET")
    @Documentation("The HTTP verb to use.")
    private HttpMethod methodType;

    @Option
    @Required
    @Documentation("End of url to complete base url of the datastore.")
    private String resource = "";

    @Option
    @Required
    @Documentation("Format of the body's answer.")
    private Format format = Format.RAW_TEXT;

    @Option
    @Documentation("If answer body type is JSON, infer numbers type or force all to double.")
    @ActiveIf(target = "format", value = "JSON")
    @DefaultValue("true")
    private boolean jsonForceDouble = true;

    @Option
    @Documentation("How many redirection are supported ? (-1 for infinite)")
    @DefaultValue("3")
    @Min(-1)
    @Required
    private Integer maxRedirect = 3;

    @Option
    @Documentation("Redirect only if same host.")
    @DefaultValue("false")
    @ActiveIf(target = "maxRedirect", value = "0", negate = true)
    private boolean only_same_host = false;

    @Option
    @Documentation("Force a GET on a 302 redirection.")
    @DefaultValue("false")
    @ActiveIf(target = "maxRedirect", value = "0", negate = true)
    private boolean force_302_redirect = false;

    @Option
    @Documentation("Does the request have parameters in URL ?")
    private boolean hasPathParams = false;

    @Option
    @ActiveIf(target = "hasPathParams", value = "true")
    @Documentation("Path parameters.")
    private List<Param> pathParams = new ArrayList<>(Collections.singleton(new Param("", "")));

    @Option
    @Documentation("Does the request have headers ?")
    private boolean hasHeaders = false;

    @Option
    @ActiveIf(target = "hasHeaders", value = "true")
    @Documentation("Query headers.")
    private List<Param> headers = new ArrayList<>(Collections.singleton(new Param("", "")));

    @Option
    @Documentation("Does the request have query parameters ?")
    private boolean hasQueryParams = false;

    @Option
    @ActiveIf(target = "hasQueryParams", value = "true")
    @Documentation("Query parameters.")
    private List<Param> queryParams = new ArrayList<>(Collections.singleton(new Param("", "")));

    @Option
    @Documentation("Does the request have a body ?")
    private boolean hasBody;

    @Option
    @ActiveIf(target = "hasBody", value = "true")
    @Documentation("Request body")
    private RequestBody body;

    @Option
    @Documentation("Return complete payload as a record")
    private boolean completePayload = false;

    public boolean supportRedirect() {
        return this.getMaxRedirect() != 0;
    }

}
