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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_DEDUPE_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_EXTERNAL_OPPORTUNITY_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ROLE;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet.OtherEntityAction;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.marketo")
class OpportunitySourceTest extends SourceBaseTest {

    public static final String TEST_OPPORTUNITY_EXISTING = "opportunity00";

    OpportunitySource source;

    @BeforeEach
    protected void setUp() {
        super.setUp();
        inputDataSet.setEntity(MarketoEntity.Opportunity);
        inputDataSet.setBatchSize(3);
    }

    private void initSource() {
        source = new OpportunitySource(inputDataSet, i18n, jsonFactory, jsonReader, jsonWriter, authorizationClient,
                opportunityClient);
        source.init();
    }

    @ParameterizedTest
    @ValueSource(strings = { "Opportunity", "OpportunityRole" })
    void testDescribeOpportunity(String entity) {
        inputDataSet.setEntity(MarketoEntity.valueOf(entity));
        inputDataSet.setOtherAction(OtherEntityAction.describe);
        initSource();
        while ((result = source.next()) != null) {
            assertNotNull(result);
            assertNotNull(result.getString(ATTR_NAME));
            assertNotNull(result.getJsonArray(ATTR_DEDUPE_FIELDS));
        }
    }

    @ParameterizedTest
    @ValueSource(strings = { "Opportunity", "OpportunityRole" })
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
}
