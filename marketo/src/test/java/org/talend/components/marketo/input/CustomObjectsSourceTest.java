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

import static org.junit.Assert.*;

import javax.json.JsonObject;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.marketo")
public class CustomObjectsSourceTest extends SourceBaseTest {

    CustomObjectSource source;

    String fields = "createdAt,marketoGUID,updatedAt,VIN,customerId,model,year";

    String CUSTOM_OBJECT_NAME = "car_c";

    JsonObject result;

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.CustomObject);
    }

    private void initSource() {
        source = new CustomObjectSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient,
                customObjectClient);
        source.init();
    }

    @Test
    void testListCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.list);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString("name"));
        }
    }

    @Test
    void testDescribeCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        initSource();
        JsonObject result = source.next();
        assertNotNull(result);
        assertEquals(fields, marketoService.getFieldsFromDescribeFormatedForApi(result.getJsonArray("fields")));
        result = source.next();
        assertNull(result);
    }

    @Test
    void testGetCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("VIN");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
    }

    @Test
    void testGetCustomObjectsWithCompoundKey() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("VIN");
        inputDataSet.setFilterValues("google01,google02,google03,google04,google05,google06");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
    }

    @Test
    void testGetCustomObjectsFails() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("billingCountry");
        inputDataSet.setFilterValues("France");
        inputDataSet.setFields("mainPhone,company,website");
        inputDataSet.setBatchSize(10);
        source = new CustomObjectSource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient,
                customObjectClient);
        try {
            source.init();
        } catch (RuntimeException e) {
            assertEquals("[1003] Invalid filterType 'billingCountry'", e.getMessage());
        }
    }

}
