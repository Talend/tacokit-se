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

import static org.junit.Assert.*;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CODE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EMAIL;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_REASONS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.OutputAction;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.SyncMethod;
import org.talend.sdk.component.api.processor.OutputEmitter;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class LeadProcessorTest extends MarketoProcessorBaseTest {

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        outputDataSet.setEntity(MarketoEntity.Lead);
        data = jsonFactory.createObjectBuilder().add(ATTR_EMAIL, "egallois@talend.com").add("firstName", "Emmanuel").build();
    }

    private void initProcessor() {
        processor = new MarketoProcessor(outputDataSet, service, tools);
        processor.init();
    }

    @Test
    void testSyncLeads() {
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);
        outputDataSet.setLookupField(ATTR_EMAIL);
        initProcessor();
        processor.map(data, main -> assertTrue(main.getInt(ATTR_ID) > 0), reject -> fail(FAIL_REJECT));
    }

    @Test
    void testSyncLeadsFail() {
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);
        outputDataSet.setLookupField(ATTR_EMAIL);
        initProcessor();
        data = jsonFactory.createObjectBuilder().add(ATTR_EMAIL, "egallois@talend.com").add("firsame", "Emmanuel").build();
        processor.map(data, main -> fail("Should not have a lead created due to invalid field."),
                reject -> assertEquals("skipped", reject.getString(ATTR_STATUS)));
    }

    @Test
    void testDeleteLead() {
        // be sure that created
        outputDataSet.setAction(OutputAction.sync);
        outputDataSet.setSyncMethod(SyncMethod.createOrUpdate);
        outputDataSet.setLookupField(ATTR_EMAIL);
        initProcessor();
        final int[] createdLead = new int[1];
        OutputEmitter<JsonObject> main = new OutputEmitter<JsonObject>() {

            @Override
            public void emit(JsonObject main) {
                createdLead[0] = main.getInt(ATTR_ID);
            }
        };
        processor.map(data, main, reject -> fail(FAIL_REJECT));
        // delete
        outputDataSet.setAction(OutputAction.delete);
        initProcessor();
        data = jsonFactory.createObjectBuilder().add(ATTR_ID, createdLead[0]).build();
        processor.map(data, mainDelete -> assertEquals("deleted", mainDelete.getString(ATTR_STATUS)),
                reject -> fail("Lead should have be deleted"));
    }

    @Test
    void testDeleteLeadFail() {
        outputDataSet.setAction(OutputAction.delete);
        initProcessor();
        data = jsonFactory.createObjectBuilder().add(ATTR_ID, 0).build();
        processor.map(data, main -> fail("Lead 0 should not exist."),
                reject -> assertEquals("1004", reject.getJsonArray(ATTR_REASONS).get(0).asJsonObject().getString(ATTR_CODE)));
    }

}
