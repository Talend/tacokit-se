/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package org.talend.components.salesforce.soql;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.junit.jupiter.api.Test;

/**
 * Unit-tests for {@link SoqlQuery} class
 */
public class SoqlQueryTest {

    /**
     * Checks {@link SoqlQuery#getFieldDescriptions()} returns correct field description in case of simple query
     */
    @Test
    public void testGetFieldDescriptions() {
        String queryString = "SELECT Name, Account.Name, Account.Owner.Name FROM Contact";
        SoqlQuery soqlQuery = SoqlQuery.getInstance();
        soqlQuery.init(queryString);
        List<FieldDescription> fieldDescriptions = soqlQuery.getFieldDescriptions();

        assertThat(fieldDescriptions, hasSize(3));

        FieldDescription field0 = fieldDescriptions.get(0);
        assertEquals("Name", field0.getSimpleName());
        assertEquals("Name", field0.getFullName());
        List<String> entityNames0 = field0.getEntityNames();
        assertThat(entityNames0, contains("Contact"));

        FieldDescription field1 = fieldDescriptions.get(1);
        assertEquals("Name", field1.getSimpleName());
        assertEquals("Account_Name", field1.getFullName());
        List<String> entityNames1 = field1.getEntityNames();
        assertThat(entityNames1, contains("Contact", "Account"));

        FieldDescription field2 = fieldDescriptions.get(2);
        assertEquals("Name", field2.getSimpleName());
        assertEquals("Account_Owner_Name", field2.getFullName());
        List<String> entityNames2 = field2.getEntityNames();
        assertThat(entityNames2, contains("Contact", "Account", "Owner"));
    }

    /**
     * Checks {@link SoqlQuery#getFieldDescriptions()} returns correct field description in case of query with WHERE and
     * WITH clauses
     */
    @Test
    public void testGetFieldDescriptionsWithRemaining() {
        String queryString = "SELECT Id FROM Contact WHERE Name LIKE 'A%' WITH DATA CATEGORY Product__c AT mobile_phones__c";
        SoqlQuery soqlQuery = SoqlQuery.getInstance();
        soqlQuery.init(queryString);
        List<FieldDescription> fieldDescriptions = soqlQuery.getFieldDescriptions();

        assertThat(fieldDescriptions, hasSize(1));

        FieldDescription field0 = fieldDescriptions.get(0);
        assertEquals("Id", field0.getSimpleName());
        assertEquals("Id", field0.getFullName());
        List<String> entityNames0 = field0.getEntityNames();
        assertThat(entityNames0, contains("Contact"));
    }

    /**
     * Checks {@link SoqlQuery#getFieldDescriptions()} returns correct field description in case of parent-to-child
     * query
     */
    @Test
    public void testGetFieldDescriptionsWithSubquery() {
        String queryString = "SELECT Name, (SELECT CreatedBy.Name FROM Notes) FROM Account";
        SoqlQuery soqlQuery = SoqlQuery.getInstance();
        soqlQuery.init(queryString);
        List<FieldDescription> fieldDescriptions = soqlQuery.getFieldDescriptions();

        assertThat(fieldDescriptions, hasSize(2));

        FieldDescription field0 = fieldDescriptions.get(0);
        assertEquals("Name", field0.getSimpleName());
        assertEquals("Name", field0.getFullName());
        List<String> entityNames0 = field0.getEntityNames();
        assertThat(entityNames0, contains("Account"));

        FieldDescription field1 = fieldDescriptions.get(1);
        assertEquals("Name", field1.getSimpleName());
        assertEquals("Notes_records_CreatedBy_Name", field1.getFullName());
        List<String> entityNames1 = field1.getEntityNames();
        assertThat(entityNames1, contains("Note", "CreatedBy"));
    }

    /**
     * Checks {@link SoqlQuery#getDrivingEntityName()} returns driving entity name, which equals name from outer FROM
     * clause
     */
    @Test
    public void testGetDrivingEntityName() {
        String queryString = "SELECT Name, (SELECT CreatedBy.Name FROM Notes) FROM Account";
        SoqlQuery soqlQuery = SoqlQuery.getInstance();
        soqlQuery.init(queryString);
        String drivingEntityName = soqlQuery.getDrivingEntityName();
        assertEquals("Account", drivingEntityName);
    }
}