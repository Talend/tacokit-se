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
package org.talend.components.marketo.input;

import javax.json.JsonObject;

import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.OpportunityClient;
import org.talend.sdk.component.api.configuration.Option;

public class OpportunitySource extends MarketoSource {

    private final OpportunityClient opportunityClient;

    public OpportunitySource(@Option("configuration") MarketoInputDataSet dataset, final I18nMessage i18n,
            final AuthorizationClient authorizationClient, final OpportunityClient opportunityClient) {
        super(dataset, i18n, authorizationClient);
        this.opportunityClient = opportunityClient;
        this.opportunityClient.base(this.dataSet.getDataStore().getEndpoint());
    }

    @Override
    public JsonObject runAction() {
        throw new RuntimeException(i18n.invalidOperation());
    }
}
