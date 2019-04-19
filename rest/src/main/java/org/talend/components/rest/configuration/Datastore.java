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
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Suggestable;
import org.talend.sdk.component.api.configuration.condition.ActiveIf;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;

@Version(1)
@Data
@DataStore("Datastore")
@Documentation("Define where is the REST API and its description.")
@GridLayout({ @GridLayout.Row({ "base" }), @GridLayout.Row({ "useDescriptor" }), @GridLayout.Row({ "descriptorUrl" }) })
public class Datastore implements Serializable {

    @Option
    @Required
    @Pattern("^https?://.+$")
    @Documentation("")
    @Suggestable(value = "getBase", parameters = { "useDescriptor", "descriptorUrl" })
    private String base;

    @Option
    @Documentation("")
    private boolean useDescriptor;

    @Option
    @Documentation("")
    @ActiveIf(target = "useDescriptor", value = "true")
    @DefaultValue("https://gist.githubusercontent.com/ypiel-talend/02330699995f9105c523cc28e7104d71/raw/4448fa5a8cd36705bb7c73e22eaa906b573d397a/gistfile1.txt")
    private String descriptorUrl;

}
