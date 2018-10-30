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
package org.talend.components.marketo.output;

import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.service.MarketoService;
import org.talend.components.marketo.service.Toolbox;

public abstract class OutputComponentStrategy extends MarketoSourceOrProcessor implements ProcessorStrategy {

    protected final MarketoOutputDataSet dataSet;

    public OutputComponentStrategy(final MarketoOutputDataSet dataSet, //
            final MarketoService service, //
            final Toolbox tools) {
        super(dataSet, service, tools);
        this.dataSet = dataSet;
    }

    @Override
    public void init() {
        super.init();
    }
}
