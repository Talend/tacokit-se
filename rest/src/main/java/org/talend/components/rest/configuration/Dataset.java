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
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(1)
@Data
@DataSet("Dataset")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "methodType" }), @GridLayout.Row({ "resource" }),
        @GridLayout.Row({ "authentication" }) })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "redirect", "force_302_redirect" }),
        @GridLayout.Row({ "connectionTimeout" }), @GridLayout.Row({ "readTimeout" }), })
@Documentation("Define the resource and authentication")
public class Dataset implements Serializable {

    @Option
    @Documentation("Identification of the REST API")
    private Datastore datastore;

    @Option
    @DefaultValue("GET")
    @Documentation("Action on the resource")
    private HttpMethod methodType;

    @Option
    @Documentation("End of url to complete base url of the datastore")
    private String resource;

    @Option
    @Documentation("")
    private Authentication authentication;

    @Min(0)
    @Option
    @Documentation("")
    private Integer connectionTimeout;

    @Min(0)
    @Option
    @Documentation("")
    private Integer readTimeout;

    @Option
    @Documentation("")
    private Boolean redirect;

    @Option
    @Documentation("")
    @ActiveIf(target = "redirect", value = "true")
    private Boolean force_302_redirect;

}
