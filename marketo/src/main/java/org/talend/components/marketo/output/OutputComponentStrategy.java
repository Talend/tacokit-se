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
package org.talend.components.marketo.output;

import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputConfiguration;
import org.talend.components.marketo.service.MarketoService;

public abstract class OutputComponentStrategy extends MarketoSourceOrProcessor implements ProcessorStrategy {

    protected final MarketoOutputConfiguration configuration;

    public OutputComponentStrategy(final MarketoOutputConfiguration configuration, //
            final MarketoService service) {
        super(configuration.getDataSet(), service);
        this.configuration = configuration;
    }

    @Override
    public void init() {
        super.init();
    }
}
