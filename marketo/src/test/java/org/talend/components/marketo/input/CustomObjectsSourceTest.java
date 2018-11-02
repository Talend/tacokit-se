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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.talend.components.marketo.dataset.CompoundKey;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
public class CustomObjectsSourceTest extends SourceBaseTest {

    CustomObjectSource source;

    String fields = "createdAt,marketoGUID,updatedAt,VIN,customerId,model,year";

    String CUSTOM_OBJECT_NAME = "car_c";

    @Override
    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.CustomObject);
        inputDataSet.setBatchSize(10);
        inputDataSet.setFields(asList(fields.split(",")));
        inputDataSet.setUseCompoundKey(false);
    }

    void initSource() {
        source = new CustomObjectSource(inputDataSet, service, tools);
        source.init();
    }

    @Test
    void testListCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.list);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_NAME));
        }
    }

    @Test
    void testDescribeCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        initSource();
        result = source.next();
        assertNotNull(result);
        // assertEquals(fields, result.getString(ATTR_FIELDS));
        // assertEquals(fields, service.getFieldsFromDescribeFormatedForApi(result.getJsonArray(ATTR_FIELDS)));
        result = source.next();
        assertNull(result);
    }

    @Test
    void testGetCustomObjects() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("marketoGUID");
        inputDataSet.setFilterValues("a215bdf6-3fed-42e5-9042-3c4258768afb");
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetCustomObjectsWithCompoundKey() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("dedupeFields");
        inputDataSet.setUseCompoundKey(true);
        List<CompoundKey> compoundKey = new ArrayList<>();
        compoundKey.add(new CompoundKey("customerId", "5"));
        compoundKey.add(new CompoundKey("VIN", "ABC-DEF-12345-GIN"));
        inputDataSet.setCompoundKey(compoundKey);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
        }
    }

    @Test
    void testGetCustomObjectsFails() {
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setCustomObjectName(CUSTOM_OBJECT_NAME);
        inputDataSet.setFilterType("billingCountry");
        inputDataSet.setFilterValues("France");
        try {
            initSource();
        } catch (RuntimeException e) {
            assertEquals("[1003] Invalid filterType 'billingCountry'", e.getMessage());
        }
    }

}
