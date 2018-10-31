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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;
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

    private MarketoInputDataSet dataset;

    private MarketoService service;

    private Toolbox tools;

    private AuthorizationClient authorizationClient;

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoInputMapper.class);

    public MarketoInputMapper(@Option("configuration") final MarketoInputDataSet dataset, //
            final MarketoService service, //
            final Toolbox tools) {
        this.dataset = dataset;
        this.service = service;
        this.tools = tools;
        authorizationClient = service.getAuthorizationClient();
        LOG.warn("[MarketoInputMapper] {}", dataset);
        authorizationClient.base(dataset.getDataStore().getEndpoint());
    }

    @PostConstruct
    public void init() {
        // NOOP
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
            return new LeadSource(dataset, service, tools);
        case List:
            return new ListSource(dataset, service, tools);
        case CustomObject:
            return new CustomObjectSource(dataset, service, tools);
        case Company:
            return new CompanySource(dataset, service, tools);
        case Opportunity:
        case OpportunityRole:
            return new OpportunitySource(dataset, service, tools);
        }
        throw new IllegalArgumentException(tools.getI18n().invalidOperation());
    }

}
