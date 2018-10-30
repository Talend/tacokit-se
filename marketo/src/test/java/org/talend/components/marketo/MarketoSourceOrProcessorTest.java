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
package org.talend.components.marketo;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.marketo.input.LeadSource;
import org.talend.sdk.component.junit5.WithComponents;

@WithComponents("org.talend.components.marketo")
class MarketoSourceOrProcessorTest extends MarketoBaseTest {

    JsonObject json;

    MarketoSourceOrProcessor sop;

    private transient static final Logger LOG = LoggerFactory.getLogger(MarketoSourceOrProcessorTest.class);

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        sop = new LeadSource(inputDataSet, service, tools);
        json = jsonFactory.createObjectBuilder().add("id", 9876).build();
    }

    @Test
    void toRecord() {
    }

    @Test
    void toJsonObject() {
    }

}
