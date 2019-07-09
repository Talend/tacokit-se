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
package org.talend.components.marketo.datastore;

import java.io.Serializable;

import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.action.Checkable;
import org.talend.sdk.component.api.configuration.action.Validable;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayouts;
import org.talend.sdk.component.api.configuration.ui.widget.Credential;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;
import lombok.ToString;

import static org.talend.components.marketo.service.UIActionService.HEALTH_CHECK;
import static org.talend.components.marketo.service.UIActionService.VALIDATION_URL_PROPERTY;

@Data
@DataStore(MarketoDataStore.NAME)
@GridLayouts({ //
        @GridLayout({ //
                @GridLayout.Row({ "endpoint" }), //
                @GridLayout.Row({ "clientId", "clientSecret" }) //
        }) //
})
@Checkable(HEALTH_CHECK)
@Documentation(MarketoDataStore.NAME)
@ToString
public class MarketoDataStore implements Serializable {

    public static final String NAME = "MarketoDataStore";

    @Option
    @Validable(VALIDATION_URL_PROPERTY)
    @Documentation("Marketo Endpoint (host only, ie: https://123-ABC-456.mktorest.com)")
    private String endpoint;

    @Option
    @Documentation("Marketo Client Id")
    private String clientId;

    @Option
    @Credential
    @Documentation("Marketo Client Secret")
    private String clientSecret;

}
