/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.talend.components.netsuite.runtime.v2019_2.model;

import org.talend.components.netsuite.runtime.model.SearchRecordTypeDesc;

import com.netsuite.webservices.v2019_2.activities.scheduling.CalendarEventSearch;
import com.netsuite.webservices.v2019_2.activities.scheduling.CalendarEventSearchAdvanced;
import com.netsuite.webservices.v2019_2.activities.scheduling.PhoneCallSearch;
import com.netsuite.webservices.v2019_2.activities.scheduling.PhoneCallSearchAdvanced;
import com.netsuite.webservices.v2019_2.activities.scheduling.ProjectTaskSearch;
import com.netsuite.webservices.v2019_2.activities.scheduling.ProjectTaskSearchAdvanced;
import com.netsuite.webservices.v2019_2.activities.scheduling.ResourceAllocationSearch;
import com.netsuite.webservices.v2019_2.activities.scheduling.ResourceAllocationSearchAdvanced;
import com.netsuite.webservices.v2019_2.activities.scheduling.TaskSearch;
import com.netsuite.webservices.v2019_2.activities.scheduling.TaskSearchAdvanced;
import com.netsuite.webservices.v2019_2.documents.filecabinet.FileSearch;
import com.netsuite.webservices.v2019_2.documents.filecabinet.FileSearchAdvanced;
import com.netsuite.webservices.v2019_2.documents.filecabinet.FolderSearch;
import com.netsuite.webservices.v2019_2.documents.filecabinet.FolderSearchAdvanced;
import com.netsuite.webservices.v2019_2.general.communication.MessageSearch;
import com.netsuite.webservices.v2019_2.general.communication.MessageSearchAdvanced;
import com.netsuite.webservices.v2019_2.general.communication.NoteSearch;
import com.netsuite.webservices.v2019_2.general.communication.NoteSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.accounting.*;
import com.netsuite.webservices.v2019_2.lists.employees.EmployeeSearch;
import com.netsuite.webservices.v2019_2.lists.employees.EmployeeSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.employees.HcmJobSearch;
import com.netsuite.webservices.v2019_2.lists.employees.HcmJobSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.employees.PayrollItemSearch;
import com.netsuite.webservices.v2019_2.lists.employees.PayrollItemSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignSearch;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.marketing.CouponCodeSearch;
import com.netsuite.webservices.v2019_2.lists.marketing.CouponCodeSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.marketing.PromotionCodeSearch;
import com.netsuite.webservices.v2019_2.lists.marketing.PromotionCodeSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.BillingAccountSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.BillingAccountSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.ContactSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.ContactSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerStatusSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerStatusSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.EntityGroupSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.EntityGroupSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.JobSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.JobSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.JobStatusSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.JobStatusSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.JobTypeSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.JobTypeSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.PartnerSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.PartnerSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.relationships.VendorSearch;
import com.netsuite.webservices.v2019_2.lists.relationships.VendorSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingCostTemplateSearch;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingCostTemplateSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingOperationTaskSearch;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingOperationTaskSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingRoutingSearch;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingRoutingSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.support.IssueSearch;
import com.netsuite.webservices.v2019_2.lists.support.IssueSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.support.SolutionSearch;
import com.netsuite.webservices.v2019_2.lists.support.SolutionSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseSearch;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.support.TopicSearch;
import com.netsuite.webservices.v2019_2.lists.support.TopicSearchAdvanced;
import com.netsuite.webservices.v2019_2.lists.website.SiteCategorySearch;
import com.netsuite.webservices.v2019_2.lists.website.SiteCategorySearchAdvanced;
import com.netsuite.webservices.v2019_2.platform.common.*;
import com.netsuite.webservices.v2019_2.setup.customization.CustomListSearch;
import com.netsuite.webservices.v2019_2.setup.customization.CustomListSearchAdvanced;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecordSearch;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecordSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.customers.ChargeSearch;
import com.netsuite.webservices.v2019_2.transactions.customers.ChargeSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemDemandPlanSearch;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemDemandPlanSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemSupplyPlanSearch;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemSupplyPlanSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.employees.PaycheckSearch;
import com.netsuite.webservices.v2019_2.transactions.employees.PaycheckSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeBillSearch;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeBillSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeEntrySearch;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeEntrySearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeSheetSearch;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeSheetSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.financial.BudgetSearch;
import com.netsuite.webservices.v2019_2.transactions.financial.BudgetSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.purchases.InboundShipmentSearch;
import com.netsuite.webservices.v2019_2.transactions.purchases.InboundShipmentSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.sales.AccountingTransactionSearch;
import com.netsuite.webservices.v2019_2.transactions.sales.AccountingTransactionSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.sales.OpportunitySearch;
import com.netsuite.webservices.v2019_2.transactions.sales.OpportunitySearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.sales.TransactionSearch;
import com.netsuite.webservices.v2019_2.transactions.sales.TransactionSearchAdvanced;
import com.netsuite.webservices.v2019_2.transactions.sales.UsageSearch;
import com.netsuite.webservices.v2019_2.transactions.sales.UsageSearchAdvanced;

public enum SearchRecordTypeEnum implements SearchRecordTypeDesc {
    ACCOUNT("account", "Account", AccountSearch.class, AccountSearchBasic.class, AccountSearchAdvanced.class),

    ACCOUNTING_PERIOD(
            "accountingPeriod",
            "AccountingPeriod",
            AccountingPeriodSearch.class,
            AccountingPeriodSearchBasic.class,
            AccountingPeriodSearchAdvanced.class),

    ACCOUNTING_TRANSACTION(
            "accountingTransaction",
            "AccountingTransaction",
            AccountingTransactionSearch.class,
            AccountingTransactionSearchBasic.class,
            AccountingTransactionSearchAdvanced.class),

    ADDRESS("address", "Address", null, AddressSearchBasic.class, null),

    BILLING_ACCOUNT(
            "billingAccount",
            "BillingAccount",
            BillingAccountSearch.class,
            BillingAccountSearchBasic.class,
            BillingAccountSearchAdvanced.class),

    BILLING_SCHEDULE(
            "billingSchedule",
            "BillingSchedule",
            BillingScheduleSearch.class,
            BillingScheduleSearchBasic.class,
            BillingScheduleSearchAdvanced.class),

    BIN("bin", "Bin", BinSearch.class, BinSearchBasic.class, BinSearchAdvanced.class),

    BOM("bom", "Bom", BomSearch.class, BomSearchBasic.class, BomSearchAdvanced.class),

    BOM_REVISION(
            "bomRevision",
            "BomRevision",
            BomRevisionSearch.class,
            BomRevisionSearchBasic.class,
            BomRevisionSearchAdvanced.class),

    BUDGET("budget", "Budget", BudgetSearch.class, BudgetSearchBasic.class, BudgetSearchAdvanced.class),

    CALENDAR_EVENT(
            "calendarEvent",
            "CalendarEvent",
            CalendarEventSearch.class,
            CalendarEventSearchBasic.class,
            CalendarEventSearchAdvanced.class),

    CAMPAIGN("campaign", "Campaign", CampaignSearch.class, CampaignSearchBasic.class, CampaignSearchAdvanced.class),

    CHARGE("charge", "Charge", ChargeSearch.class, ChargeSearchBasic.class, ChargeSearchAdvanced.class),

    CLASSIFICATION(
            "classification",
            "Classification",
            ClassificationSearch.class,
            ClassificationSearchBasic.class,
            ClassificationSearchAdvanced.class),

    CONTACT("contact", "Contact", ContactSearch.class, ContactSearchBasic.class, ContactSearchAdvanced.class),

    CONTACT_CATEGORY(
            "contactCategory",
            "ContactCategory",
            ContactCategorySearch.class,
            ContactCategorySearchBasic.class,
            ContactCategorySearchAdvanced.class),

    CONTACT_ROLE(
            "contactRole",
            "ContactRole",
            ContactRoleSearch.class,
            ContactRoleSearchBasic.class,
            ContactRoleSearchAdvanced.class),

    CONSOLIDATED_EXCHANGE_RATE(
            "consolidatedExchangeRate",
            "ConsolidatedExchangeRate",
            ConsolidatedExchangeRateSearch.class,
            ConsolidatedExchangeRateSearchBasic.class,
            ConsolidatedExchangeRateSearchAdvanced.class),

    COST_CATEGORY(
            "costCategory",
            "CostCategory",
            CostCategorySearch.class,
            CostCategorySearchBasic.class,
            CostCategorySearchAdvanced.class),

    COUPON_CODE("couponCode", "CouponCode", CouponCodeSearch.class, CouponCodeSearchBasic.class, CouponCodeSearchAdvanced.class),

    CURRENCY_RATE(
            "currencyRate",
            "CurrencyRate",
            CurrencyRateSearch.class,
            CurrencyRateSearchBasic.class,
            CurrencyRateSearchAdvanced.class),

    CUSTOMER("customer", "Customer", CustomerSearch.class, CustomerSearchBasic.class, CustomerSearchAdvanced.class),

    CUSTOMER_CATEGORY(
            "customerCategory",
            "CustomerCategory",
            CustomerCategorySearch.class,
            CustomerCategorySearchBasic.class,
            CustomerCategorySearchAdvanced.class),

    CUSTOMER_MESSAGE(
            "customerMessage",
            "CustomerMessage",
            CustomerMessageSearch.class,
            CustomerMessageSearchBasic.class,
            CustomerMessageSearchAdvanced.class),

    CUSTOMER_STATUS(
            "customerStatus",
            "CustomerStatus",
            CustomerStatusSearch.class,
            CustomerStatusSearchBasic.class,
            CustomerStatusSearchAdvanced.class),

    CUSTOM_LIST("customList", "CustomList", CustomListSearch.class, CustomListSearchBasic.class, CustomListSearchAdvanced.class),

    CUSTOM_RECORD(
            "customRecord",
            "CustomRecord",
            CustomRecordSearch.class,
            CustomRecordSearchBasic.class,
            CustomRecordSearchAdvanced.class),

    DEPARTMENT("department", "Department", DepartmentSearch.class, DepartmentSearchBasic.class, DepartmentSearchAdvanced.class),

    EMPLOYEE("employee", "Employee", EmployeeSearch.class, EmployeeSearchBasic.class, EmployeeSearchAdvanced.class),

    ENTITY_GROUP(
            "entityGroup",
            "EntityGroup",
            EntityGroupSearch.class,
            EntityGroupSearchBasic.class,
            EntityGroupSearchAdvanced.class),

    EXPENSE_CATEGORY(
            "expenseCategory",
            "ExpenseCategory",
            ExpenseCategorySearch.class,
            ExpenseCategorySearchBasic.class,
            ExpenseCategorySearchAdvanced.class),

    FAIR_VALUE_PRICE(
            "fairValuePrice",
            "FairValuePrice",
            FairValuePriceSearch.class,
            FairValuePriceSearchBasic.class,
            FairValuePriceSearchAdvanced.class),

    FILE("file", "File", FileSearch.class, FileSearchBasic.class, FileSearchAdvanced.class),

    FOLDER("folder", "Folder", FolderSearch.class, FolderSearchBasic.class, FolderSearchAdvanced.class),

    GIFT_CERTIFICATE(
            "giftCertificate",
            "GiftCertificate",
            GiftCertificateSearch.class,
            GiftCertificateSearchBasic.class,
            GiftCertificateSearchAdvanced.class),

    GLOBAL_ACCOUNT_MAPPING(
            "globalAccountMapping",
            "GlobalAccountMapping",
            GlobalAccountMappingSearch.class,
            GlobalAccountMappingSearchBasic.class,
            GlobalAccountMappingSearchAdvanced.class),

    HCM_JOB("hcmJob", "HcmJob", HcmJobSearch.class, HcmJobSearchBasic.class, HcmJobSearchAdvanced.class),

    INBOUND_SHIPMENT(
            "inboundShipment",
            "InboundShipment",
            InboundShipmentSearch.class,
            InboundShipmentSearchBasic.class,
            InboundShipmentSearchAdvanced.class),

    INVENTORY_DETAIL("inventoryDetail", "InventoryDetail", null, InventoryDetailSearchBasic.class, null),

    INVENTORY_NUMBER(
            "inventoryNumber",
            "InventoryNumber",
            InventoryNumberSearch.class,
            InventoryNumberSearchBasic.class,
            InventoryNumberSearchAdvanced.class),

    ISSUE("issue", "Issue", IssueSearch.class, IssueSearchBasic.class, IssueSearchAdvanced.class),

    ITEM("item", "Item", ItemSearch.class, ItemSearchBasic.class, ItemSearchAdvanced.class),

    ITEM_ACCOUNT_MAPPING(
            "itemAccountMapping",
            "ItemAccountMapping",
            ItemAccountMappingSearch.class,
            ItemAccountMappingSearchBasic.class,
            ItemAccountMappingSearchAdvanced.class),

    ITEM_DEMAND_PLAN(
            "itemDemandPlan",
            "ItemDemandPlan",
            ItemDemandPlanSearch.class,
            ItemDemandPlanSearchBasic.class,
            ItemDemandPlanSearchAdvanced.class),

    ITEM_REVISION(
            "itemRevision",
            "ItemRevision",
            ItemRevisionSearch.class,
            ItemRevisionSearchBasic.class,
            ItemRevisionSearchAdvanced.class),

    ITEM_SUPPLY_PLAN(
            "itemSupplyPlan",
            "ItemSupplyPlan",
            ItemSupplyPlanSearch.class,
            ItemSupplyPlanSearchBasic.class,
            ItemSupplyPlanSearchAdvanced.class),

    JOB("job", "Job", JobSearch.class, JobSearchBasic.class, JobSearchAdvanced.class),

    JOB_STATUS("jobStatus", "JobStatus", JobStatusSearch.class, JobStatusSearchBasic.class, JobStatusSearchAdvanced.class),

    JOB_TYPE("jobType", "JobType", JobTypeSearch.class, JobTypeSearchBasic.class, JobTypeSearchAdvanced.class),

    LOCATION("location", "Location", LocationSearch.class, LocationSearchBasic.class, LocationSearchAdvanced.class),

    MANUFACTURING_COST_TEMPLATE(
            "manufacturingCostTemplate",
            "ManufacturingCostTemplate",
            ManufacturingCostTemplateSearch.class,
            ManufacturingCostTemplateSearchBasic.class,
            ManufacturingCostTemplateSearchAdvanced.class),

    MANUFACTURING_OPERATION_TASK(
            "manufacturingOperationTask",
            "ManufacturingOperationTask",
            ManufacturingOperationTaskSearch.class,
            ManufacturingOperationTaskSearchBasic.class,
            ManufacturingOperationTaskSearchAdvanced.class),

    MANUFACTURING_ROUTING(
            "manufacturingRouting",
            "ManufacturingRouting",
            ManufacturingRoutingSearch.class,
            ManufacturingRoutingSearchBasic.class,
            ManufacturingRoutingSearchAdvanced.class),

    MESSAGE("message", "Message", MessageSearch.class, MessageSearchBasic.class, MessageSearchAdvanced.class),

    NEXUS("nexus", "Nexus", NexusSearch.class, NexusSearchBasic.class, NexusSearchAdvanced.class),

    NOTE("note", "Note", NoteSearch.class, NoteSearchBasic.class, NoteSearchAdvanced.class),

    NOTE_TYPE("noteType", "NoteType", NoteTypeSearch.class, NoteTypeSearchBasic.class, NoteTypeSearchAdvanced.class),

    OPPORTUNITY(
            "opportunity",
            "Opportunity",
            OpportunitySearch.class,
            OpportunitySearchBasic.class,
            OpportunitySearchAdvanced.class),

    OTHER_NAME_CATEGORY(
            "otherNameCategory",
            "OtherNameCategory",
            OtherNameCategorySearch.class,
            OtherNameCategorySearchBasic.class,
            OtherNameCategorySearchAdvanced.class),

    PARTNER("partner", "Partner", PartnerSearch.class, PartnerSearchBasic.class, PartnerSearchAdvanced.class),

    PARTNER_CATEGORY(
            "partnerCategory",
            "PartnerCategory",
            PartnerCategorySearch.class,
            PartnerCategorySearchBasic.class,
            PartnerCategorySearchAdvanced.class),

    PAYCHECK("paycheck", "Paycheck", PaycheckSearch.class, PaycheckSearchBasic.class, PaycheckSearchAdvanced.class),

    PAYMENT_METHOD(
            "paymentMethod",
            "PaymentMethod",
            PaymentMethodSearch.class,
            PaymentMethodSearchBasic.class,
            PaymentMethodSearchAdvanced.class),

    PAYROLL_ITEM(
            "payrollItem",
            "PayrollItem",
            PayrollItemSearch.class,
            PayrollItemSearchBasic.class,
            PayrollItemSearchAdvanced.class),

    PHONE_CALL("phoneCall", "PhoneCall", PhoneCallSearch.class, PhoneCallSearchBasic.class, PhoneCallSearchAdvanced.class),

    PRICE_LEVEL("priceLevel", "PriceLevel", PriceLevelSearch.class, PriceLevelSearchBasic.class, PriceLevelSearchAdvanced.class),

    PRICING_GROUP(
            "pricingGroup",
            "PricingGroup",
            PricingGroupSearch.class,
            PricingGroupSearchBasic.class,
            PricingGroupSearchAdvanced.class),

    PROJECT_TASK(
            "projectTask",
            "ProjectTask",
            ProjectTaskSearch.class,
            ProjectTaskSearchBasic.class,
            ProjectTaskSearchAdvanced.class),

    PROMOTION_CODE(
            "promotionCode",
            "PromotionCode",
            PromotionCodeSearch.class,
            PromotionCodeSearchBasic.class,
            PromotionCodeSearchAdvanced.class),

    RESOURCE_ALLOCATION(
            "resourceAllocation",
            "ResourceAllocation",
            ResourceAllocationSearch.class,
            ResourceAllocationSearchBasic.class,
            ResourceAllocationSearchAdvanced.class),

    REV_REC_SCHEDULE(
            "revRecSchedule",
            "RevRecSchedule",
            RevRecScheduleSearch.class,
            RevRecScheduleSearchBasic.class,
            RevRecScheduleSearchAdvanced.class),

    REV_REC_TEMPLATE(
            "revRecTemplate",
            "RevRecTemplate",
            RevRecTemplateSearch.class,
            RevRecTemplateSearchBasic.class,
            RevRecTemplateSearchAdvanced.class),

    SALES_ROLE("salesRole", "SalesRole", SalesRoleSearch.class, SalesRoleSearchBasic.class, SalesRoleSearchAdvanced.class),

    SALES_TAX_ITEM(
            "salesTaxItem",
            "SalesTaxItem",
            SalesTaxItemSearch.class,
            SalesTaxItemSearchBasic.class,
            SalesTaxItemSearchAdvanced.class),

    SITE_CATEGORY(
            "siteCategory",
            "SiteCategory",
            SiteCategorySearch.class,
            SiteCategorySearchBasic.class,
            SiteCategorySearchAdvanced.class),

    SOLUTION("solution", "Solution", SolutionSearch.class, SolutionSearchBasic.class, SolutionSearchAdvanced.class),

    SUBSIDIARY("subsidiary", "Subsidiary", SubsidiarySearch.class, SubsidiarySearchBasic.class, SubsidiarySearchAdvanced.class),

    SUPPORT_CASE(
            "supportCase",
            "SupportCase",
            SupportCaseSearch.class,
            SupportCaseSearchBasic.class,
            SupportCaseSearchAdvanced.class),

    TASK("task", "Task", TaskSearch.class, TaskSearchBasic.class, TaskSearchAdvanced.class),

    TAX_GROUP("taxGroup", "TaxGroup", TaxGroupSearch.class, TaxGroupSearchBasic.class, TaxGroupSearchAdvanced.class),

    TAX_TYPE("taxType", "TaxType", TaxTypeSearch.class, TaxTypeSearchBasic.class, TaxTypeSearchAdvanced.class),

    TERM("term", "Term", TermSearch.class, TermSearchBasic.class, TermSearchAdvanced.class),

    TIME_BILL("timeBill", "TimeBill", TimeBillSearch.class, TimeBillSearchBasic.class, TimeBillSearchAdvanced.class),

    TIME_ENTRY("timeEntry", "TimeEntry", TimeEntrySearch.class, TimeEntrySearchBasic.class, TimeEntrySearchAdvanced.class),

    TIME_SHEET("timeSheet", "TimeSheet", TimeSheetSearch.class, TimeSheetSearchBasic.class, TimeSheetSearchAdvanced.class),

    TOPIC("topic", "Topic", TopicSearch.class, TopicSearchBasic.class, TopicSearchAdvanced.class),

    TRANSACTION(
            "transaction",
            "Transaction",
            TransactionSearch.class,
            TransactionSearchBasic.class,
            TransactionSearchAdvanced.class),

    UNITS_TYPE("unitsType", "UnitsType", UnitsTypeSearch.class, UnitsTypeSearchBasic.class, UnitsTypeSearchAdvanced.class),

    USAGE("usage", "Usage", UsageSearch.class, UsageSearchBasic.class, UsageSearchAdvanced.class),

    VENDOR("vendor", "Vendor", VendorSearch.class, VendorSearchBasic.class, VendorSearchAdvanced.class),

    VENDOR_CATEGORY(
            "vendorCategory",
            "VendorCategory",
            VendorCategorySearch.class,
            VendorCategorySearchBasic.class,
            VendorCategorySearchAdvanced.class),

    WIN_LOSS_REASON(
            "winLossReason",
            "WinLossReason",
            WinLossReasonSearch.class,
            WinLossReasonSearchBasic.class,
            WinLossReasonSearchAdvanced.class);

    private final String type;

    private final String typeName;

    private final Class<?> searchClass;

    private final Class<?> searchBasicClass;

    private final Class<?> searchAdvancedClass;

    SearchRecordTypeEnum(String type, String typeName, Class<?> searchClass, Class<?> searchBasicClass,
            Class<?> searchAdvancedClass) {
        this.type = type;
        this.typeName = typeName;
        this.searchClass = searchClass;
        this.searchBasicClass = searchBasicClass;
        this.searchAdvancedClass = searchAdvancedClass;
    }

    @Override
    public String getType() {
        return this.type;
    }

    @Override
    public String getTypeName() {
        return this.typeName;
    }

    @Override
    public Class<?> getSearchClass() {
        return this.searchClass;
    }

    @Override
    public Class<?> getSearchBasicClass() {
        return this.searchBasicClass;
    }

    @Override
    public Class<?> getSearchAdvancedClass() {
        return this.searchAdvancedClass;
    }

    public static SearchRecordTypeEnum getByTypeName(String typeName) {
        for (SearchRecordTypeEnum value : values()) {
            if (value.typeName.equals(typeName)) {
                return value;
            }
        }
        return null;
    }
}
