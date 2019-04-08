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
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.talend.components.rest.configuration.RequestBody.Type.X_WWW_FORM_URLENCODED;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "methodType" }), @GridLayout.Row({ "hasHeaders" }),
        @GridLayout.Row({ "headers" }), @GridLayout.Row({ "hasQueryParam" }), @GridLayout.Row({ "queryParams" }),
        @GridLayout.Row({ "body" }), })
public class RequestConfig implements Serializable {

    @Option
    @Documentation("Identification of the resource to access")
    private Dataset dataset;

    @Option
    @DefaultValue("GET")
    @Documentation("Action on the resource")
    private HttpMethod methodType;

    @Option
    @Documentation("Http request contains headers")
    private Boolean hasHeaders = false;

    @Option
    @ActiveIf(target = "hasHeaders", value = "true")
    @Documentation("Http request headers")
    private List<Param> headers = new ArrayList<>();

    @Option
    @Documentation("Http request contains query params")
    private Boolean hasQueryParam = false;

    @Option
    @ActiveIf(target = "hasQueryParam", value = "true")
    @Documentation("Http request query params")
    private List<Param> queryParams = new ArrayList<>();

    @Option
    @ActiveIf(target = "methodType", value = { "POST", "PUT", "PATCH", "DELETE", "OPTIONS" })
    @Documentation("")
    private RequestBody body;

    public Map<String, String> queryParams() {
        return queryParams.stream().collect(toMap(Param::getKey, Param::getValue));
    }

    public Map<String, String> headers() {
        final Map<String, String> h = headers.stream().collect(toMap(Param::getKey, Param::getValue));
        if (body != null && hasPayLoad() && X_WWW_FORM_URLENCODED.equals(body.getType())) {
            h.put("Content-Type", "application/x-www-form-urlencoded");
        }
        return h;
    }

    public boolean hasPayLoad() {
        switch (body.getType()) {
        case RAW:
            return body.getRawValue() != null && !body.getRawValue().isEmpty();
        case BINARY:
            return body.getBinaryPath() != null && !body.getBinaryPath().isEmpty();
        case X_WWW_FORM_URLENCODED:
        case FORM_DATA:
            return body.getParams() != null && !body.getParams().isEmpty();
        default:
            return false;
        }
    }

}
