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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit-tests for {@link SoqlQueryBuilder} class
 */
public class SoqlQueryBuilderTest {

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name
     */
    @Test
    @DisplayName("Test build SOQL query simple")
    public void testBuildSoqlQuerySimple() {
        String expected = "\"SELECT Id, Name, BillingCity FROM Account\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Id", "Name", "BillingCity"), "Account").buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * child-to-parent relationship
     */
    @Test
    @DisplayName("Test build SOQL query child to parent")
    public void testBuildSoqlQueryChildToParent() {
        String expected = "\"SELECT Name, Account.Name, Account.Owner.Name FROM Contact\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Name", "Account_Name", "Account_Owner_Name"), "Contact")
                .buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * parent-to-child relationship
     */
    @Test
    @DisplayName("Test build SOQL query parent to child")
    public void testBuildSoqlQueryParentToChild() {
        String expected = "\"SELECT Name, (SELECT LastName FROM Contacts) FROM Account\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Name", "Contacts_records_LastName"), "Account")
                .buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * parent-to-child relationship in case of three-level entities linking
     */
    @Test
    @DisplayName("Test build SOQL query parent to child depth")
    public void testBuildSoqlQueryParentToChildDepth() {
        String expected = "\"SELECT Name, (SELECT LastName, Account.Owner.Name FROM Contacts) FROM Account\"";

        String queryFromBuilder = new SoqlQueryBuilder(
                Arrays.asList("Name", "Contacts_records_LastName", "Contacts_records_Account_Owner_Name"), "Account")
                        .buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * complex relationship
     */
    @Test
    @DisplayName("Test build SOQL query complect relationship")
    public void testBuildSoqlQueryComplexRelationship() {
        String expected = "\"SELECT Id, Name, (SELECT Quantity, ListPrice, PricebookEntry.UnitPrice, PricebookEntry.Name FROM OpportunityLineItems) FROM Opportunity\"";
        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Id", "Name", "OpportunityLineItems_records_Quantity",
                "OpportunityLineItems_records_ListPrice", "OpportunityLineItems_records_PricebookEntry_UnitPrice",
                "OpportunityLineItems_records_PricebookEntry_Name"), "Opportunity").buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * custom field
     */
    @Test
    @DisplayName("Test build SOQL query custom filed")
    public void testBuildSoqlQueryCustomField() {
        String expected = "\"SELECT Id, SLAExpirationDate__c FROM Account\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Id", "SLAExpirationDate__c"), "Account").buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * custom table
     */
    @Test
    @DisplayName("Test build SOQL query with custom module")
    public void testBuildSoqlQueryWithCustomTable() {
        String expected = "\"SELECT Id, SLAExpirationDate__c FROM talend_custom__c\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Id", "SLAExpirationDate__c"), "talend_custom__c")
                .buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * custom fields/table and relation child to parent
     */
    @Test
    @DisplayName("Test build SOQL query with custom modules child to parent")
    public void testBuildSoqlQueryWithCustomTablesChildToParent() {
        String expected = "\"SELECT talend_custom__c.Name, talend_custom__c.custom_name__c, talend_contact__r.Name, talend_contact__r.Account.Name FROM talend_custom__c\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("talend_custom__c_Name", "talend_custom__c_custom_name__c",
                "talend_contact__r_Name", "talend_contact__r_Account_Name"), "talend_custom__c").buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * custom fields/table and relation parent to child
     */
    @Test
    @DisplayName("Test build SOQL query with custom modules parent to child")
    public void testBuildSoqlQueryWithCustomTablesParentToChild() {
        String expected = "\"SELECT Name, (SELECT custom.lastName, custom_name__c, talend_account__c.custom_lastName__c, talend_account__c.Age FROM talend_custom__r), contact_title__c FROM talend_contact__c\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Name", "talend_custom__r_records_custom_lastName",
                "contact_title__c", "talend_custom__r_records_custom_name__c",
                "talend_custom__r_records_talend_account__c_custom_lastName__c",
                "talend_custom__r_records_talend_account__c_Age"), "talend_contact__c").buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }

    /**
     * Checks {@link SoqlQueryBuilder#buildSoqlQuery()} returns SOQL query according to column list and entity name with
     * custom fields, and complex table name with relation parent to child
     */
    @Test
    @DisplayName("Test build SOQL query with complex child table name parent to child")
    public void testBuildSoqlQueryWithComplexChildTableNameParentToChild() {
        String expected = "\"SELECT Name, (SELECT custom.lastName, talend_account__c.Age FROM talend_contact__c.Persons), contact_title__c FROM talend_contact__c\"";

        String queryFromBuilder = new SoqlQueryBuilder(Arrays.asList("Name", "talend_contact__c_Persons_records_custom_lastName",
                "contact_title__c", "talend_contact__c_Persons_records_talend_account__c_Age"), "talend_contact__c")
                        .buildSoqlQuery();

        Assert.assertEquals(expected, queryFromBuilder);
    }
}
