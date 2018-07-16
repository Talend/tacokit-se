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

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;
import javax.json.JsonBuilderFactory;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.CampaignClient;
import org.talend.components.marketo.service.CompanyClient;
import org.talend.components.marketo.service.CustomObjectClient;
import org.talend.components.marketo.service.I18nMessage;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.OpportunityClient;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "MarketoInput")
@PartitionMapper(family = "Marketo", name = "Input")
@Documentation("Marketo Input Component")
public class MarketoInputMapper implements Serializable {

    private final I18nMessage i18n;

    private final MarketoInputDataSet dataset;

    private final JsonBuilderFactory jsonFactory;

    private final JsonReaderFactory jsonReader;

    private final JsonWriterFactory jsonWriter;

    private final AuthorizationClient authorizationClient;

    private final LeadClient leadClient;

    private final ListClient listClient;

    private final CustomObjectClient customObjectClient;

    private final CampaignClient campaignClient;

    private final OpportunityClient opportunityClient;

    private final CompanyClient companyClient;

    public MarketoInputMapper(@Option("configuration") final MarketoInputDataSet dataset, //
            final I18nMessage i18n, //
            final JsonBuilderFactory jsonFactory, //
            final JsonReaderFactory jsonReader, //
            final JsonWriterFactory jsonWriter, //
            final AuthorizationClient authorizationClient, //
            final LeadClient leadClient, //
            final ListClient listClient, //
            final CustomObjectClient customObjectClient, //
            final CampaignClient campaignClient, //
            final OpportunityClient opportunityClient, //
            final CompanyClient companyClient //
    ) {
        this.dataset = dataset;
        this.i18n = i18n;
        this.jsonFactory = jsonFactory;
        this.jsonReader = jsonReader;
        this.jsonWriter = jsonWriter;

        this.authorizationClient = authorizationClient;
        this.leadClient = leadClient;
        this.customObjectClient = customObjectClient;
        this.campaignClient = campaignClient;
        this.opportunityClient = opportunityClient;
        this.companyClient = companyClient;
        this.listClient = listClient;
    }

    @PostConstruct
    public void init() {
        authorizationClient.base(dataset.getDataStore().getEndpoint());
    }

    @Assessor
    public long estimateSize() {
        return 300L;
    }

    @Split
    public List<MarketoInputMapper> split(@PartitionSize final long bundles) {
        return Collections.singletonList(this);
    }

    @Emitter
    public MarketoSource createWorker() {
        switch (dataset.getEntity()) {
        case Lead:
            return new LeadSource(dataset, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, leadClient);
        case List:
            return new ListSource(dataset, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, listClient);
        case CustomObject:
            return new CustomObjectSource(dataset, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient,
                    customObjectClient);
        case Company:
            return new CompanySource(dataset, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient, companyClient);
        case Opportunity:
        case OpportunityRole:
            return new OpportunitySource(dataset, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient,
                    opportunityClient);
        }
        throw new IllegalArgumentException(i18n.invalidOperation());
    }

}
