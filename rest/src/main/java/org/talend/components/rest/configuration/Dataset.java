// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
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
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "redirect", "force_302_redirect" }),
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
    @DefaultValue("100")
    private Integer connectionTimeout;

    @Min(0)
    @Option
    @Required
    @Documentation("")
    @DefaultValue("100")
    private Integer readTimeout;

    @Option
    @Documentation("")
    @DefaultValue("false")
    private Boolean redirect;

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
