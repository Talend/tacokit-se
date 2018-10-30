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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoOutputDataSet.ListAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class ListProcessorTest extends MarketoProcessorBaseTest {

    private static final int LEAD_ID = 5;

    private static final int LEAD_ID_ADDREMOVE = 4;

    private static final int LIST_ID = 1001;

    private static final int LEAD_ID_INVALID = -100;

    JsonObject dataInvalidLead;

    private JsonObject dataAddRemove;

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        outputDataSet.setEntity(MarketoEntity.List);
        data = jsonFactory.createObjectBuilder().add(ATTR_LIST_ID, LIST_ID).add(ATTR_LEAD_ID, LEAD_ID).build();
        dataAddRemove = jsonFactory.createObjectBuilder().add(ATTR_LIST_ID, LIST_ID).add(ATTR_LEAD_ID, LEAD_ID_ADDREMOVE).build();
        dataInvalidLead = jsonFactory.createObjectBuilder().add(ATTR_LIST_ID, LIST_ID).add(ATTR_LEAD_ID, LEAD_ID_INVALID).build();
    }

    private void initProcessor() {
        processor = new MarketoProcessor(outputDataSet, service, tools);
        processor.init();
    }

    @Test
    void testIsMemberOfList() {
        outputDataSet.setListAction(ListAction.isMemberOf);
        initProcessor();
        processor.map(data, main -> {
            assertEquals("memberof", main.getString(ATTR_STATUS));
        }, reject -> {
            fail(FAIL_REJECT);
        });
    }

    @Test
    void testIsMemberOfListNotMember() {
        outputDataSet.setListAction(ListAction.isMemberOf);
        initProcessor();
        processor.map(dataAddRemove, main -> {
            assertEquals("notmemberof", main.getString(ATTR_STATUS));
        }, reject -> {
            fail(FAIL_REJECT);
        });
    }

    @Test
    void testIsMemberOfListFail() {
        outputDataSet.setListAction(ListAction.isMemberOf);
        initProcessor();
        // {"id":-100,"status":"skipped","reasons":[{"code":"1004","message":"Lead not found"}]}
        processor.map(dataInvalidLead, main -> {
            fail("Should not be in List");
        }, reject -> {
            assertEquals("skipped", reject.getString(ATTR_STATUS));
        });
    }

    @Test
    void testAddToList() {
        outputDataSet.setListAction(ListAction.removeFrom);
        initProcessor();
        processor.map(dataAddRemove, main -> {

        }, reject -> {
        });
        //
        outputDataSet.setListAction(ListAction.addTo);
        initProcessor();
        processor.map(dataAddRemove, main -> {
            assertEquals("added", main.getString(ATTR_STATUS));
        }, reject -> {
            fail(FAIL_REJECT);
        });
    }

    @Test
    void testRemoveFromList() {
        outputDataSet.setListAction(ListAction.addTo);
        initProcessor();
        processor.map(dataAddRemove, main -> {

        }, reject -> {
        });
        //
        outputDataSet.setListAction(ListAction.removeFrom);
        initProcessor();
        processor.map(dataAddRemove, main -> {
            assertEquals("removed", main.getString(ATTR_STATUS));
        }, reject -> {
            fail(FAIL_REJECT);
        });
    }

}
