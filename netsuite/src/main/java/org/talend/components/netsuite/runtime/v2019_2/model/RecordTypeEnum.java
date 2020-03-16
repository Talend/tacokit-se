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

import org.talend.components.netsuite.runtime.model.RecordTypeDesc;

import com.netsuite.webservices.v2019_2.activities.scheduling.CalendarEvent;
import com.netsuite.webservices.v2019_2.activities.scheduling.PhoneCall;
import com.netsuite.webservices.v2019_2.activities.scheduling.ProjectTask;
import com.netsuite.webservices.v2019_2.activities.scheduling.ResourceAllocation;
import com.netsuite.webservices.v2019_2.activities.scheduling.Task;
import com.netsuite.webservices.v2019_2.documents.filecabinet.File;
import com.netsuite.webservices.v2019_2.documents.filecabinet.Folder;
import com.netsuite.webservices.v2019_2.general.communication.Message;
import com.netsuite.webservices.v2019_2.general.communication.Note;
import com.netsuite.webservices.v2019_2.lists.accounting.*;
import com.netsuite.webservices.v2019_2.lists.employees.Employee;
import com.netsuite.webservices.v2019_2.lists.employees.HcmJob;
import com.netsuite.webservices.v2019_2.lists.employees.PayrollItem;
import com.netsuite.webservices.v2019_2.lists.marketing.Campaign;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignAudience;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignCategory;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignChannel;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignFamily;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignOffer;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignResponse;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignSearchEngine;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignSubscription;
import com.netsuite.webservices.v2019_2.lists.marketing.CampaignVertical;
import com.netsuite.webservices.v2019_2.lists.marketing.CouponCode;
import com.netsuite.webservices.v2019_2.lists.marketing.PromotionCode;
import com.netsuite.webservices.v2019_2.lists.relationships.BillingAccount;
import com.netsuite.webservices.v2019_2.lists.relationships.Contact;
import com.netsuite.webservices.v2019_2.lists.relationships.Customer;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerStatus;
import com.netsuite.webservices.v2019_2.lists.relationships.CustomerSubsidiaryRelationship;
import com.netsuite.webservices.v2019_2.lists.relationships.EntityGroup;
import com.netsuite.webservices.v2019_2.lists.relationships.Job;
import com.netsuite.webservices.v2019_2.lists.relationships.JobStatus;
import com.netsuite.webservices.v2019_2.lists.relationships.JobType;
import com.netsuite.webservices.v2019_2.lists.relationships.Partner;
import com.netsuite.webservices.v2019_2.lists.relationships.Vendor;
import com.netsuite.webservices.v2019_2.lists.relationships.VendorSubsidiaryRelationship;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingCostTemplate;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingOperationTask;
import com.netsuite.webservices.v2019_2.lists.supplychain.ManufacturingRouting;
import com.netsuite.webservices.v2019_2.lists.support.Issue;
import com.netsuite.webservices.v2019_2.lists.support.Solution;
import com.netsuite.webservices.v2019_2.lists.support.SupportCase;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseIssue;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseOrigin;
import com.netsuite.webservices.v2019_2.lists.support.SupportCasePriority;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseStatus;
import com.netsuite.webservices.v2019_2.lists.support.SupportCaseType;
import com.netsuite.webservices.v2019_2.lists.support.Topic;
import com.netsuite.webservices.v2019_2.lists.website.SiteCategory;
import com.netsuite.webservices.v2019_2.platform.common.Address;
import com.netsuite.webservices.v2019_2.platform.common.InventoryDetail;
import com.netsuite.webservices.v2019_2.platform.common.LandedCost;
import com.netsuite.webservices.v2019_2.setup.customization.CrmCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.CustomList;
import com.netsuite.webservices.v2019_2.setup.customization.CustomPurchase;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecord;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecordCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.CustomRecordType;
import com.netsuite.webservices.v2019_2.setup.customization.CustomSale;
import com.netsuite.webservices.v2019_2.setup.customization.CustomSegment;
import com.netsuite.webservices.v2019_2.setup.customization.CustomTransaction;
import com.netsuite.webservices.v2019_2.setup.customization.EntityCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.ItemCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.ItemNumberCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.ItemOptionCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.OtherCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.TransactionBodyCustomField;
import com.netsuite.webservices.v2019_2.setup.customization.TransactionColumnCustomField;
import com.netsuite.webservices.v2019_2.transactions.bank.Check;
import com.netsuite.webservices.v2019_2.transactions.bank.Deposit;
import com.netsuite.webservices.v2019_2.transactions.customers.CashRefund;
import com.netsuite.webservices.v2019_2.transactions.customers.Charge;
import com.netsuite.webservices.v2019_2.transactions.customers.CreditMemo;
import com.netsuite.webservices.v2019_2.transactions.customers.CustomerDeposit;
import com.netsuite.webservices.v2019_2.transactions.customers.CustomerPayment;
import com.netsuite.webservices.v2019_2.transactions.customers.CustomerRefund;
import com.netsuite.webservices.v2019_2.transactions.customers.DepositApplication;
import com.netsuite.webservices.v2019_2.transactions.customers.ReturnAuthorization;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemDemandPlan;
import com.netsuite.webservices.v2019_2.transactions.demandplanning.ItemSupplyPlan;
import com.netsuite.webservices.v2019_2.transactions.employees.ExpenseReport;
import com.netsuite.webservices.v2019_2.transactions.employees.Paycheck;
import com.netsuite.webservices.v2019_2.transactions.employees.PaycheckJournal;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeBill;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeEntry;
import com.netsuite.webservices.v2019_2.transactions.employees.TimeSheet;
import com.netsuite.webservices.v2019_2.transactions.financial.Budget;
import com.netsuite.webservices.v2019_2.transactions.general.AdvInterCompanyJournalEntry;
import com.netsuite.webservices.v2019_2.transactions.general.InterCompanyJournalEntry;
import com.netsuite.webservices.v2019_2.transactions.general.JournalEntry;
import com.netsuite.webservices.v2019_2.transactions.general.PeriodEndJournal;
import com.netsuite.webservices.v2019_2.transactions.general.StatisticalJournalEntry;
import com.netsuite.webservices.v2019_2.transactions.inventory.AssemblyBuild;
import com.netsuite.webservices.v2019_2.transactions.inventory.AssemblyUnbuild;
import com.netsuite.webservices.v2019_2.transactions.inventory.BinTransfer;
import com.netsuite.webservices.v2019_2.transactions.inventory.BinWorksheet;
import com.netsuite.webservices.v2019_2.transactions.inventory.InterCompanyTransferOrder;
import com.netsuite.webservices.v2019_2.transactions.inventory.InventoryAdjustment;
import com.netsuite.webservices.v2019_2.transactions.inventory.InventoryCostRevaluation;
import com.netsuite.webservices.v2019_2.transactions.inventory.InventoryTransfer;
import com.netsuite.webservices.v2019_2.transactions.inventory.TransferOrder;
import com.netsuite.webservices.v2019_2.transactions.inventory.WorkOrder;
import com.netsuite.webservices.v2019_2.transactions.inventory.WorkOrderClose;
import com.netsuite.webservices.v2019_2.transactions.inventory.WorkOrderCompletion;
import com.netsuite.webservices.v2019_2.transactions.inventory.WorkOrderIssue;
import com.netsuite.webservices.v2019_2.transactions.purchases.InboundShipment;
import com.netsuite.webservices.v2019_2.transactions.purchases.ItemReceipt;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseOrder;
import com.netsuite.webservices.v2019_2.transactions.purchases.PurchaseRequisition;
import com.netsuite.webservices.v2019_2.transactions.purchases.VendorBill;
import com.netsuite.webservices.v2019_2.transactions.purchases.VendorCredit;
import com.netsuite.webservices.v2019_2.transactions.purchases.VendorPayment;
import com.netsuite.webservices.v2019_2.transactions.purchases.VendorReturnAuthorization;
import com.netsuite.webservices.v2019_2.transactions.sales.CashSale;
import com.netsuite.webservices.v2019_2.transactions.sales.Estimate;
import com.netsuite.webservices.v2019_2.transactions.sales.Invoice;
import com.netsuite.webservices.v2019_2.transactions.sales.ItemFulfillment;
import com.netsuite.webservices.v2019_2.transactions.sales.Opportunity;
import com.netsuite.webservices.v2019_2.transactions.sales.SalesOrder;
import com.netsuite.webservices.v2019_2.transactions.sales.Usage;

public enum RecordTypeEnum implements RecordTypeDesc {
    ACCOUNT("account", "Account", Account.class, "account"),
    ACCOUNTING_PERIOD("accountingPeriod", "AccountingPeriod", AccountingPeriod.class, "accountingPeriod"),
    ADDRESS("address", "Address", Address.class, "address"),
    ADV_INTER_COMPANY_JOURNAL_ENTRY(
            "advInterCompanyJournalEntry",
            "AdvInterCompanyJournalEntry",
            AdvInterCompanyJournalEntry.class,
            "transaction"),
    ASSEMBLY_BUILD("assemblyBuild", "AssemblyBuild", AssemblyBuild.class, "transaction"),
    ASSEMBLY_ITEM("assemblyItem", "AssemblyItem", AssemblyItem.class, "item"),
    ASSEMBLY_UNBUILD("assemblyUnbuild", "AssemblyUnbuild", AssemblyUnbuild.class, "transaction"),
    BILLING_ACCOUNT("billingAccount", "BillingAccount", BillingAccount.class, "billingAccount"),
    BILLING_SCHEDULE("billingSchedule", "BillingSchedule", BillingSchedule.class, "billingSchedule"),
    BIN("bin", "Bin", Bin.class, "bin"),
    BIN_TRANSFER("binTransfer", "BinTransfer", BinTransfer.class, "transaction"),
    BIN_WORKSHEET("binWorksheet", "BinWorksheet", BinWorksheet.class, "transaction"),
    BOM("bom", "Bom", Bom.class, "bom"),
    BOM_REVISION("bomRevision", "BomRevision", BomRevision.class, "bomRevision"),
    BUDGET("budget", "Budget", Budget.class, "budget"),
    BUDGET_CATEGORY("budgetCategory", "BudgetCategory", BudgetCategory.class, "item"),
    CALENDAR_EVENT("calendarEvent", "CalendarEvent", CalendarEvent.class, "calendarEvent"),
    CAMPAIGN("campaign", "Campaign", Campaign.class, "campaign"),
    CAMPAIGN_AUDIENCE("campaignAudience", "CampaignAudience", CampaignAudience.class, "item"),
    CAMPAIGN_CATEGORY("campaignCategory", "CampaignCategory", CampaignCategory.class, "item"),
    CAMPAIGN_CHANNEL("campaignChannel", "CampaignChannel", CampaignChannel.class, "item"),
    CAMPAIGN_FAMILY("campaignFamily", "CampaignFamily", CampaignFamily.class, "item"),
    CAMPAIGN_OFFER("campaignOffer", "CampaignOffer", CampaignOffer.class, "item"),
    CAMPAIGN_RESPONSE("campaignResponse", "CampaignResponse", CampaignResponse.class, "item"),
    CAMPAIGN_SEARCH_ENGINE("campaignSearchEngine", "CampaignSearchEngine", CampaignSearchEngine.class, "item"),
    CAMPAIGN_SUBSCRIPTION("campaignSubscription", "CampaignSubscription", CampaignSubscription.class, "item"),
    CAMPAIGN_VERTICAL("campaignVertical", "CampaignVertical", CampaignVertical.class, "item"),
    CASH_REFUND("cashRefund", "CashRefund", CashRefund.class, "transaction"),
    CASH_SALE("cashSale", "CashSale", CashSale.class, "transaction"),
    CHARGE("charge", "Charge", Charge.class, "charge"),
    CHECK("check", "Check", Check.class, "transaction"),
    CLASSIFICATION("classification", "Classification", Classification.class, "classification"),
    CONSOLIDATED_EXCHANGE_RATE(
            "consolidatedExchangeRate",
            "ConsolidatedExchangeRate",
            ConsolidatedExchangeRate.class,
            "consolidatedExchangeRate"),
    CONTACT("contact", "Contact", Contact.class, "contact"),
    CONTACT_CATEGORY("contactCategory", "ContactCategory", ContactCategory.class, "contactCategory"),
    CONTACT_ROLE("contactRole", "ContactRole", ContactRole.class, "contactRole"),
    COST_CATEGORY("costCategory", "CostCategory", CostCategory.class, "costCategory"),
    COUPON_CODE("couponCode", "CouponCode", CouponCode.class, "couponCode"),
    CREDIT_MEMO("creditMemo", "CreditMemo", CreditMemo.class, "transaction"),
    CRM_CUSTOM_FIELD("crmCustomField", "CrmCustomField", CrmCustomField.class, null),
    CURRENCY("currency", "Currency", Currency.class, "item"),
    CURRENCY_RATE("currencyRate", "CurrencyRate", CurrencyRate.class, "currencyRate"),
    CUSTOMER("customer", "Customer", Customer.class, "customer"),
    CUSTOMER_CATEGORY("customerCategory", "CustomerCategory", CustomerCategory.class, "customerCategory"),
    CUSTOMER_DEPOSIT("customerDeposit", "CustomerDeposit", CustomerDeposit.class, "transaction"),
    CUSTOMER_MESSAGE("customerMessage", "CustomerMessage", CustomerMessage.class, "customerMessage"),
    CUSTOMER_PAYMENT("customerPayment", "CustomerPayment", CustomerPayment.class, "transaction"),
    CUSTOMER_REFUND("customerRefund", "CustomerRefund", CustomerRefund.class, "transaction"),
    CUSTOMER_STATUS("customerStatus", "CustomerStatus", CustomerStatus.class, "customerStatus"),
    CUSTOMER_SUBSIDIARY_RELATIONSHIP(
            "customerSubsidiaryRelationship",
            "CustomerSubsidiaryRelationship",
            CustomerSubsidiaryRelationship.class,
            "customerSubsidiaryRelationship"),
    CUSTOM_LIST("customList", "CustomList", CustomList.class, "customList"),
    CUSTOM_PURCHASE("customPurchase", "CustomPurchase", CustomPurchase.class, "transaction"),
    CUSTOM_RECORD("customRecord", "CustomRecord", CustomRecord.class, "customRecord"),
    CUSTOM_RECORD_CUSTOM_FIELD("customRecordCustomField", "CustomRecordCustomField", CustomRecordCustomField.class, null),
    CUSTOM_RECORD_TYPE("customRecordType", "CustomRecordType", CustomRecordType.class, null),
    CUSTOM_SALE("customSale", "CustomSale", CustomSale.class, "transaction"),
    CUSTOM_SEGMENT("customSegment", "CustomSegment", CustomSegment.class, null),
    CUSTOM_TRANSACTION("customTransaction", "CustomTransaction", CustomTransaction.class, "transaction"),
    DEPARTMENT("department", "Department", Department.class, "department"),
    DEPOSIT("deposit", "Deposit", Deposit.class, "transaction"),
    DEPOSIT_APPLICATION("depositApplication", "DepositApplication", DepositApplication.class, "transaction"),
    DESCRIPTION_ITEM("descriptionItem", "DescriptionItem", DescriptionItem.class, "item"),
    DISCOUNT_ITEM("discountItem", "DiscountItem", DiscountItem.class, "item"),
    DOWNLOAD_ITEM("downloadItem", "DownloadItem", DownloadItem.class, "item"),
    EMPLOYEE("employee", "Employee", Employee.class, "employee"),
    ENTITY_CUSTOM_FIELD("entityCustomField", "EntityCustomField", EntityCustomField.class, null),
    ENTITY_GROUP("entityGroup", "EntityGroup", EntityGroup.class, "entityGroup"),
    ESTIMATE("estimate", "Estimate", Estimate.class, "transaction"),
    EXPENSE_CATEGORY("expenseCategory", "ExpenseCategory", ExpenseCategory.class, "expenseCategory"),
    EXPENSE_REPORT("expenseReport", "ExpenseReport", ExpenseReport.class, "transaction"),
    FAIR_VALUE_PRICE("fairValuePrice", "FairValuePrice", FairValuePrice.class, "fairValuePrice"),
    FILE("file", "File", File.class, "file"),
    FOLDER("folder", "Folder", Folder.class, "folder"),
    GENERAL_TOKEN("generalToken", "GeneralToken", GeneralToken.class, null),
    GIFT_CERTIFICATE("giftCertificate", "GiftCertificate", GiftCertificate.class, "giftCertificate"),
    GIFT_CERTIFICATE_ITEM("giftCertificateItem", "GiftCertificateItem", GiftCertificateItem.class, "item"),
    GLOBAL_ACCOUNT_MAPPING("globalAccountMapping", "GlobalAccountMapping", GlobalAccountMapping.class, "globalAccountMapping"),
    HCM_JOB("hcmJob", "HcmJob", HcmJob.class, "hcmJob"),
    INBOUND_SHIPMENT("inboundShipment", "InboundShipment", InboundShipment.class, "inboundShipment"),
    INTER_COMPANY_JOURNAL_ENTRY(
            "interCompanyJournalEntry",
            "InterCompanyJournalEntry",
            InterCompanyJournalEntry.class,
            "transaction"),
    INTER_COMPANY_TRANSFER_ORDER(
            "interCompanyTransferOrder",
            "InterCompanyTransferOrder",
            InterCompanyTransferOrder.class,
            "transaction"),
    INVENTORY_ADJUSTMENT("inventoryAdjustment", "InventoryAdjustment", InventoryAdjustment.class, "transaction"),
    INVENTORY_COST_REVALUATION("inventoryCostRevaluation", "InventoryCostRevaluation", InventoryCostRevaluation.class, null),
    INVENTORY_DETAIL("inventoryDetail", "InventoryDetail", InventoryDetail.class, "inventoryDetail"),
    INVENTORY_ITEM("inventoryItem", "InventoryItem", InventoryItem.class, "item"),
    INVENTORY_NUMBER("inventoryNumber", "InventoryNumber", InventoryNumber.class, "inventoryNumber"),
    INVENTORY_TRANSFER("inventoryTransfer", "InventoryTransfer", InventoryTransfer.class, "transaction"),
    INVOICE("invoice", "Invoice", Invoice.class, "transaction"),
    ISSUE("issue", "Issue", Issue.class, "issue"),
    ITEM_ACCOUNT_MAPPING("itemAccountMapping", "ItemAccountMapping", ItemAccountMapping.class, "itemAccountMapping"),
    ITEM_CUSTOM_FIELD("itemCustomField", "ItemCustomField", ItemCustomField.class, null),
    ITEM_DEMAND_PLAN("itemDemandPlan", "ItemDemandPlan", ItemDemandPlan.class, "itemDemandPlan"),
    ITEM_FULFILLMENT("itemFulfillment", "ItemFulfillment", ItemFulfillment.class, "transaction"),
    ITEM_GROUP("itemGroup", "ItemGroup", ItemGroup.class, "item"),
    ITEM_NUMBER_CUSTOM_FIELD("itemNumberCustomField", "ItemNumberCustomField", ItemNumberCustomField.class, null),
    ITEM_OPTION_CUSTOM_FIELD("itemOptionCustomField", "ItemOptionCustomField", ItemOptionCustomField.class, null),
    ITEM_RECEIPT("itemReceipt", "ItemReceipt", ItemReceipt.class, "transaction"),
    ITEM_REVISION("itemRevision", "ItemRevision", ItemRevision.class, "itemRevision"),
    ITEM_SUPPLY_PLAN("itemSupplyPlan", "ItemSupplyPlan", ItemSupplyPlan.class, "itemSupplyPlan"),
    JOB("job", "Job", Job.class, "job"),
    JOB_STATUS("jobStatus", "JobStatus", JobStatus.class, "jobStatus"),
    JOB_TYPE("jobType", "JobType", JobType.class, "jobType"),
    JOURNAL_ENTRY("journal", "JournalEntry", JournalEntry.class, "transaction"),
    KIT_ITEM("kitItem", "KitItem", KitItem.class, "item"),
    LANDED_COST("landedCost", "LandedCost", LandedCost.class, null),
    LEAD_SOURCE("leadSource", "LeadSource", LeadSource.class, "item"),
    LOCATION("location", "Location", Location.class, "location"),
    LOT_NUMBERED_ASSEMBLY_ITEM("lotNumberedAssemblyItem", "LotNumberedAssemblyItem", LotNumberedAssemblyItem.class, "item"),
    LOT_NUMBERED_INVENTORY_ITEM("lotNumberedInventoryItem", "LotNumberedInventoryItem", LotNumberedInventoryItem.class, "item"),
    MANUFACTURING_COST_TEMPLATE(
            "manufacturingCostTemplate",
            "ManufacturingCostTemplate",
            ManufacturingCostTemplate.class,
            "manufacturingCostTemplate"),
    MANUFACTURING_OPERATION_TASK(
            "manufacturingOperationTask",
            "ManufacturingOperationTask",
            ManufacturingOperationTask.class,
            "manufacturingOperationTask"),
    MANUFACTURING_ROUTING("manufacturingRouting", "ManufacturingRouting", ManufacturingRouting.class, "manufacturingRouting"),
    MARKUP_ITEM("markupItem", "MarkupItem", MarkupItem.class, "item"),
    MERCHANDISE_HIERARCHY_NODE(
            "merchandiseHierarchyNode",
            "MerchandiseHierarchyNode",
            MerchandiseHierarchyNode.class,
            "merchandiseHierarchyNode"),
    MESSAGE("message", "Message", Message.class, "message"),
    NEXUS("nexus", "Nexus", Nexus.class, "nexus"),
    NON_INVENTORY_PURCHASE_ITEM("nonInventoryPurchaseItem", "NonInventoryPurchaseItem", NonInventoryPurchaseItem.class, "item"),
    NON_INVENTORY_RESALE_ITEM("nonInventoryResaleItem", "NonInventoryResaleItem", NonInventoryResaleItem.class, "item"),
    NON_INVENTORY_SALE_ITEM("nonInventorySaleItem", "NonInventorySaleItem", NonInventorySaleItem.class, "item"),
    NOTE("note", "Note", Note.class, "note"),
    NOTE_TYPE("noteType", "NoteType", NoteType.class, "noteType"),
    OPPORTUNITY("opportunity", "Opportunity", Opportunity.class, "opportunity"),
    OTHER_CHARGE_PURCHASE_ITEM("otherChargePurchaseItem", "OtherChargePurchaseItem", OtherChargePurchaseItem.class, "item"),
    OTHER_CHARGE_RESALE_ITEM("otherChargeResaleItem", "OtherChargeResaleItem", OtherChargeResaleItem.class, "item"),
    OTHER_CHARGE_SALE_ITEM("otherChargeSaleItem", "OtherChargeSaleItem", OtherChargeSaleItem.class, "item"),
    OTHER_CUSTOM_FIELD("otherCustomField", "OtherCustomField", OtherCustomField.class, null),
    OTHER_NAME_CATEGORY("otherNameCategory", "OtherNameCategory", OtherNameCategory.class, "otherNameCategory"),
    PARTNER("partner", "Partner", Partner.class, "partner"),
    PARTNER_CATEGORY("partnerCategory", "PartnerCategory", PartnerCategory.class, "partnerCategory"),
    PAYCHECK("paycheck", "Paycheck", Paycheck.class, "paycheck"),
    PAYCHECK_JOURNAL("paycheckJournal", "PaycheckJournal", PaycheckJournal.class, "transaction"),
    PAYMENT_CARD("paymentCard", "PaymentCard", PaymentCard.class, null),
    PAYMENT_CARD_TOKEN("paymentCardToken", "PaymentCardToken", PaymentCardToken.class, null),
    PAYMENT_ITEM("paymentItem", "PaymentItem", PaymentItem.class, "item"),
    PAYMENT_METHOD("paymentMethod", "PaymentMethod", PaymentMethod.class, "paymentMethod"),
    PAYROLL_ITEM("payrollItem", "PayrollItem", PayrollItem.class, "payrollItem"),
    PERIOD_END_JOURNAL("periodEndJournal", "PeriodEndJournal", PeriodEndJournal.class, "transaction"),
    PHONE_CALL("phoneCall", "PhoneCall", PhoneCall.class, "phoneCall"),
    PRICE_LEVEL("priceLevel", "PriceLevel", PriceLevel.class, "priceLevel"),
    PRICING_GROUP("pricingGroup", "PricingGroup", PricingGroup.class, "pricingGroup"),
    PROJECT_TASK("projectTask", "ProjectTask", ProjectTask.class, "projectTask"),
    PROMOTION_CODE("promotionCode", "PromotionCode", PromotionCode.class, "promotionCode"),
    PURCHASE_ORDER("purchaseOrder", "PurchaseOrder", PurchaseOrder.class, "transaction"),
    PURCHASE_REQUISITION("purchaseRequisition", "PurchaseRequisition", PurchaseRequisition.class, null),
    RESOURCE_ALLOCATION("resourceAllocation", "ResourceAllocation", ResourceAllocation.class, "resourceAllocation"),
    RETURN_AUTHORIZATION("returnAuthorization", "ReturnAuthorization", ReturnAuthorization.class, "transaction"),
    REV_REC_SCHEDULE("revRecSchedule", "RevRecSchedule", RevRecSchedule.class, "revRecSchedule"),
    REV_REC_TEMPLATE("revRecTemplate", "RevRecTemplate", RevRecTemplate.class, "revRecTemplate"),
    SALES_ORDER("salesOrder", "SalesOrder", SalesOrder.class, "transaction"),
    SALES_ROLE("salesRole", "SalesRole", SalesRole.class, "salesRole"),
    SALES_TAX_ITEM("salesTaxItem", "SalesTaxItem", SalesTaxItem.class, "salesTaxItem"),
    SERIALIZED_ASSEMBLY_ITEM("serializedAssemblyItem", "SerializedAssemblyItem", SerializedAssemblyItem.class, "item"),
    SERIALIZED_INVENTORY_ITEM("serializedInventoryItem", "SerializedInventoryItem", SerializedInventoryItem.class, "item"),
    SERVICE_PURCHASE_ITEM("servicePurchaseItem", "ServicePurchaseItem", ServicePurchaseItem.class, "item"),
    SERVICE_RESALE_ITEM("serviceResaleItem", "ServiceResaleItem", ServiceResaleItem.class, "item"),
    SERVICE_SALE_ITEM("serviceSaleItem", "ServiceSaleItem", ServiceSaleItem.class, "item"),
    SITE_CATEGORY("siteCategory", "SiteCategory", SiteCategory.class, "siteCategory"),
    SOLUTION("solution", "Solution", Solution.class, "solution"),
    STATE("state", "State", State.class, "transaction"),
    STATISTICAL_JOURNAL_ENTRY("statisticalJournalEntry", "StatisticalJournalEntry", StatisticalJournalEntry.class, "item"),
    SUBSIDIARY("subsidiary", "Subsidiary", Subsidiary.class, "subsidiary"),
    SUBTOTAL_ITEM("subtotalItem", "SubtotalItem", SubtotalItem.class, "item"),
    SUPPORT_CASE("supportCase", "SupportCase", SupportCase.class, "supportCase"),
    SUPPORT_CASE_ISSUE("supportCaseIssue", "SupportCaseIssue", SupportCaseIssue.class, "item"),
    SUPPORT_CASE_ORIGIN("supportCaseOrigin", "SupportCaseOrigin", SupportCaseOrigin.class, "item"),
    SUPPORT_CASE_PRIORITY("supportCasePriority", "SupportCasePriority", SupportCasePriority.class, "item"),
    SUPPORT_CASE_STATUS("supportCaseStatus", "SupportCaseStatus", SupportCaseStatus.class, "item"),
    SUPPORT_CASE_TYPE("supportCaseType", "SupportCaseType", SupportCaseType.class, "item"),
    TASK("task", "Task", Task.class, "task"),
    TAX_ACCT("taxAcct", "TaxAcct", TaxAcct.class, "item"),
    TAX_GROUP("taxGroup", "TaxGroup", TaxGroup.class, "taxGroup"),
    TAX_TYPE("taxType", "TaxType", TaxType.class, "taxType"),
    TERM("term", "Term", Term.class, "term"),
    TIME_BILL("timeBill", "TimeBill", TimeBill.class, "timeBill"),
    TIME_ENTRY("timeEntry", "TimeEntry", TimeEntry.class, "timeEntry"),
    TIME_SHEET("timeSheet", "TimeSheet", TimeSheet.class, "timeSheet"),
    TOPIC("topic", "Topic", Topic.class, "topic"),
    TRANSACTION_BODY_CUSTOM_FIELD(
            "transactionBodyCustomField",
            "TransactionBodyCustomField",
            TransactionBodyCustomField.class,
            null),
    TRANSACTION_COLUMN_CUSTOM_FIELD(
            "transactionColumnCustomField",
            "TransactionColumnCustomField",
            TransactionColumnCustomField.class,
            null),
    TRANSFER_ORDER("transferOrder", "TransferOrder", TransferOrder.class, "transaction"),
    UNITS_TYPE("unitsType", "UnitsType", UnitsType.class, "unitsType"),
    USAGE("usage", "Usage", Usage.class, "usage"),
    VENDOR("vendor", "Vendor", Vendor.class, "vendor"),
    VENDOR_BILL("vendorBill", "VendorBill", VendorBill.class, "transaction"),
    VENDOR_CATEGORY("vendorCategory", "VendorCategory", VendorCategory.class, "vendorCategory"),
    VENDOR_CREDIT("vendorCredit", "VendorCredit", VendorCredit.class, "transaction"),
    VENDOR_PAYMENT("vendorPayment", "VendorPayment", VendorPayment.class, "transaction"),
    VENDOR_RETURN_AUTHORIZATION(
            "vendorReturnAuthorization",
            "VendorReturnAuthorization",
            VendorReturnAuthorization.class,
            "transaction"),
    VENDOR_SUBSIDIARY_RELATIONSHIP(
            "vendorSubsidiaryRelationship",
            "VendorSubsidiaryRelationship",
            VendorSubsidiaryRelationship.class,
            "vendorSubsidiaryRelationship"),
    WIN_LOSS_REASON("winLossReason", "WinLossReason", WinLossReason.class, "winLossReason"),
    WORK_ORDER("workOrder", "WorkOrder", WorkOrder.class, "transaction"),
    WORK_ORDER_CLOSE("workOrderClose", "WorkOrderClose", WorkOrderClose.class, "transaction"),
    WORK_ORDER_COMPLETION("workOrderCompletion", "WorkOrderCompletion", WorkOrderCompletion.class, "transaction"),
    WORK_ORDER_ISSUE("workOrderIssue", "WorkOrderIssue", WorkOrderIssue.class, "transaction");

    private final String type;

    private final String typeName;

    private final Class<?> recordClass;

    private final String searchRecordType;

    RecordTypeEnum(String type, String typeName, Class<?> recordClass, String searchRecordType) {
        this.type = type;
        this.typeName = typeName;
        this.recordClass = recordClass;
        this.searchRecordType = searchRecordType;
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
    public Class<?> getRecordClass() {
        return this.recordClass;
    }

    @Override
    public String getSearchRecordType() {
        return this.searchRecordType;
    }

    public static RecordTypeEnum getByTypeName(String typeName) {
        for (RecordTypeEnum value : values()) {
            if (value.typeName.equals(typeName)) {
                return value;
            }
        }
        return null;
    }
}
