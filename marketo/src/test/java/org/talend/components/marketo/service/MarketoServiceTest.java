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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;

import javax.json.JsonObject;
import javax.json.JsonReader;

import org.junit.jupiter.api.Test;
import org.talend.components.marketo.MarketoBaseTest;
import org.talend.sdk.component.junit.http.junit5.HttpApi;
import org.talend.sdk.component.junit5.WithComponents;

@HttpApi(useSsl = true)
@WithComponents("org.talend.components.marketo")
class MarketoServiceTest extends MarketoBaseTest {

    @Test
    void getFieldsFromDescribeFormatedForApi() {
        final String fields = "createdAt,externalCompanyId,id,updatedAt,annualRevenue,billingCity,billingCountry,"
                + "billingPostalCode,billingState,billingStreet,company,companyNotes,externalSalesPersonId,industry,"
                + "mainPhone,numberOfEmployees,sicCode,site,website";

        JsonReader reader = jsonReader.createReader(getClass().getClassLoader().getResourceAsStream("describe_company.json"));
        JsonObject v = reader.readObject();
        String f = marketoService
                .getFieldsFromDescribeFormatedForApi(v.getJsonArray("result").get(0).asJsonObject().getJsonArray(ATTR_FIELDS));
        assertEquals(fields, f);
    }

}
