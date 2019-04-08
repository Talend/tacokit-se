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
import org.talend.sdk.component.api.configuration.constraint.Min;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(1)
@Data
@DataSet("Dataset")
@GridLayout({ @GridLayout.Row({ "datastore" }), @GridLayout.Row({ "authentication" }), })
@GridLayout(names = GridLayout.FormType.ADVANCED, value = { @GridLayout.Row({ "connectionTimeout" }),
        @GridLayout.Row({ "readTimeout" }), })
@Documentation("Define the resource and authentication")
public class Dataset implements Serializable {

    @Option
    @Documentation("Identification of the REST API")
    private Datastore datastore;

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

}
