// ============================================================================
//
// Copyright (C) 2006-2018 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.datastore;

import lombok.Data;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

@Data
@DataStore(MarketoDataStore.NAME)
@GridLayout({ //
        @GridLayout.Row({ "endpoint" }), //
        @GridLayout.Row({ "clientId", "clientSecret" }) //
})
@Checkable("marketo_healthcheck")
@Documentation(MarketoDataStore.NAME)
public class MarketoDataStore {

    public static final String NAME = "MarketoDataStore";

    @Option
    @Required
    @Validable("urlValidation")
    @Documentation("Marketo Endpoint (host only, ie: https://123-ABC-456.mktorest.com)")
    private String endpoint;

    @Option
    @Required
    @Documentation("Marketo Client Id")
    private String clientId;

    @Option
    @Required
    @Credential
    @Documentation("Marketo Client Secret")
    private String clientSecret;

}
