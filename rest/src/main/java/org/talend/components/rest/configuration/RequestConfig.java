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
import org.talend.sdk.component.api.configuration.action.Updatable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toMap;
import static org.talend.components.rest.configuration.RequestBody.Type.X_WWW_FORM_URLENCODED;

@Data
@GridLayout({ @GridLayout.Row({ "dataset" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "dataset" }), @GridLayout.Row({ "stopIfNotOk" }) })
public class RequestConfig implements Serializable {

    @Option
    @Documentation("Identification of the resource to access")
    private Dataset dataset;

    @Option
    @Documentation("Raise an error if the HTTP status code is not 200")
    @DefaultValue("false")
    private boolean stopIfNotOk;

    public Map<String, String> pathParams() {
        if (!getDataset().getHasPathParams()) {
            return new HashMap<String, String>();
        }

        return dataset.getPathParams().stream().collect(toMap(Param::getKey, Param::getValue));
    }

    public Map<String, String> queryParams() {
        if (!getDataset().getHasQueryParams()) {
            return new HashMap<String, String>();
        }

        return dataset.getQueryParams().stream().collect(toMap(Param::getKey, Param::getValue));
    }

    public Map<String, String> headers() {
        final Map<String, String> h = new HashMap<String, String>();
        if (dataset.getBody() != null && hasPayLoad() && X_WWW_FORM_URLENCODED.equals(dataset.getBody().getType())) {
            h.put("Content-Type", "application/x-www-form-urlencoded");
        }

        if (!getDataset().getHasHeaders()) {
            return h;
        }

        h.putAll(dataset.getHeaders().stream().collect(toMap(Param::getKey, Param::getValue)));
        return h;
    }

    public boolean hasPayLoad() {
        switch (dataset.getBody().getType()) {
        case RAW:
            return dataset.getBody().getRawValue() != null && !dataset.getBody().getRawValue().isEmpty();
        case BINARY:
            return dataset.getBody().getBinaryPath() != null && !dataset.getBody().getBinaryPath().isEmpty();
        case X_WWW_FORM_URLENCODED:
        case FORM_DATA:
            return dataset.getBody().getParams() != null && !dataset.getBody().getParams().isEmpty();
        default:
            return false;
        }
    }

    public RequestBody body() {
        RequestBody _body = null;
        if (hasPayLoad()) {
            _body = this.dataset.getBody();
        }

        return _body;
    }

}
