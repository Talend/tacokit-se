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
import static org.junit.Assert.*;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EXTERNAL_OPPORTUNITY_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ROLE;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.marketo.dataset.CompoundKey;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true, responseLocator = org.talend.sdk.component.junit.http.internal.impl.MarketoResponseLocator.class)
@WithComponents("org.talend.components.marketo")
class OpportunitySourceTest extends SourceBaseTest {

    public static final String TEST_OPPORTUNITY_EXISTING = "opportunity00";

    OpportunitySource source;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Opportunity);
        inputDataSet.setBatchSize(3);
        inputDataSet.setUseCompoundKey(false);
    }

    private void initSource() {
        source = new OpportunitySource(inputDataSet, service, tools);
        source.init();
    }

    @ParameterizedTest
    @ValueSource(strings = { "Opportunity" })
    void testDescribeOpportunity(String entity) {
        inputDataSet.setEntity(MarketoEntity.valueOf(entity));
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_NAME));
            assertNotNull(result.getString(ATTR_DEDUPE_FIELDS));
        }
    }

    @Test
    void testDescribeOpportunityRole() {
        inputDataSet.setEntity(MarketoEntity.OpportunityRole);
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_NAME));
            assertNotNull(result.getString(ATTR_DEDUPE_FIELDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "Opportunity" })
    void testGetOpportunities(String entity) {
        inputDataSet.setEntity(MarketoEntity.valueOf(entity));
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType(ATTR_EXTERNAL_OPPORTUNITY_ID);
        inputDataSet.setFilterValues(TEST_OPPORTUNITY_EXISTING);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_EXTERNAL_OPPORTUNITY_ID));
            if ("OpportunityRole".equals(entity)) {
                assertNotNull(result.getInt(ATTR_LEAD_ID));
                assertNotNull(result.getString(ATTR_ROLE));
            }
        }
    }

    @Test
    void testGetOpportunityRoles() {
        inputDataSet.setEntity(MarketoEntity.OpportunityRole);
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFilterType(ATTR_EXTERNAL_OPPORTUNITY_ID);
        inputDataSet.setFilterValues(TEST_OPPORTUNITY_EXISTING);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_EXTERNAL_OPPORTUNITY_ID));
            assertNotNull(result.getInt(ATTR_LEAD_ID));
            assertNotNull(result.getString(ATTR_ROLE));
        }
    }

    @Test
    void testGetOpportunityRolesWithCompoundKey() {
        inputDataSet.setEntity(MarketoEntity.OpportunityRole);
        inputDataSet.setOtherAction(OtherEntityAction.get);
        inputDataSet.setFields(asList("marketoGuid", "externalOpportunityId", "leadId", "role"));
        inputDataSet.setFilterType("dedupeFields");
        inputDataSet.setUseCompoundKey(true);
        List<CompoundKey> compoundKey = new ArrayList<>();
        compoundKey.add(new CompoundKey(ATTR_EXTERNAL_OPPORTUNITY_ID, "opportunity102"));
        compoundKey.add(new CompoundKey(ATTR_LEAD_ID, "4"));
        compoundKey.add(new CompoundKey(ATTR_ROLE, "newCust"));
        inputDataSet.setCompoundKey(compoundKey);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_EXTERNAL_OPPORTUNITY_ID));
            assertNotNull(result.getInt(ATTR_LEAD_ID));
            assertNotNull(result.getString(ATTR_ROLE));
        }
    }
}
