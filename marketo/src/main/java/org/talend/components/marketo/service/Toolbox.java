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
package org.talend.components.marketo.service;

import javax.json.JsonBuilderFactory;
import javax.json.JsonReaderFactory;
import javax.json.JsonWriterFactory;

import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import lombok.Getter;
import lombok.experimental.Accessors;

@Accessors
@Service
public class Toolbox {

    @Getter
    @Service
    private I18nMessage i18n;

    @Getter
    @Service
    private AuthorizationClient authorizationClient;

    @Getter
    @Service
    private JsonBuilderFactory jsonFactory;

    @Getter
    @Service
    private JsonReaderFactory jsonReader;

    @Getter
    @Service
    private JsonWriterFactory jsonWriter;

    @Getter
    @Service
    protected RecordBuilderFactory recordBuilder;
}
