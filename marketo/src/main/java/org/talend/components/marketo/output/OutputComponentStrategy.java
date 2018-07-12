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

import javax.json.JsonBuilderFactory;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.talend.components.marketo.MarketoSourceOrProcessor;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.service.AuthorizationClient;
import org.talend.components.marketo.service.I18nMessage;

public abstract class OutputComponentStrategy extends MarketoSourceOrProcessor implements ProcessorStrategy {

    protected final MarketoOutputDataSet dataSet;

    protected final JsonBuilderFactory jsonFactory;

    protected final JsonReaderFactory jsonReader;

    protected final JsonWriterFactory jsonWriter;

    public OutputComponentStrategy(final MarketoOutputDataSet dataSet, final I18nMessage i18n,
            final AuthorizationClient authorizationClient, final JsonBuilderFactory jsonFactory,
            final JsonReaderFactory jsonReader, JsonWriterFactory jsonWriter) {
        super(dataSet, i18n, authorizationClient);
        this.dataSet = dataSet;
        this.jsonFactory = jsonFactory;
        this.jsonReader = jsonReader;
        this.jsonWriter = jsonWriter;
    }

    @Override
    public void init() {
        super.init();
    }
}
