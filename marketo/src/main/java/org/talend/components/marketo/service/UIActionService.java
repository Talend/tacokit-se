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

import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.dataset.MarketoInputDataSet.ListAction;
import org.talend.components.marketo.dataset.MarketoOutputDataSet;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Schema.Entry;

@Service
public class UIActionService extends MarketoService {

    public static final String ACTIVITIES_LIST = "ACTIVITIES_LIST";

    public static final String LEAD_KEY_NAME_LIST = "LEAD_KEY_NAME_LIST";

    public static final String LIST_NAMES = "LIST_NAMES";

    public static final String FIELD_NAMES = "FIELD_NAMES";

    public static final String CUSTOM_OBJECT_NAMES = "CUSTOM_OBJECT_NAMES";

    public static final String FIELD_NAME_PROPOSITIONS = "FIELD_NAME_PROPOSITIONS";

    public static final String HEALTH_CHECK = "MARKETO_HEALTH_CHECK";

    public static final String URL_CHECK = "MARKETO_URL_CHECK";

    public static final String GUESS_ENTITY_SCHEMA_INPUT = "guessEntitySchemaForInput";

    public static final String GUESS_ENTITY_SCHEMA_OUTPUT = "guessEntitySchemaForOutput";

    private transient static final Logger LOG = getLogger(UIActionService.class);

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus doHealthCheck(@Option(MarketoDataStore.NAME) MarketoDataStore dataStore, final I18nMessage i18n) {
        initClients(dataStore);
        Response<JsonObject> result = authorizationClient.getAuthorizationToken(CLIENT_CREDENTIALS, dataStore.getClientId(),
                dataStore.getClientSecret());
        if (result.status() == 200 && result.body().getString(ATTR_ACCESS_TOKEN, null) != null) {
            return new HealthCheckStatus(HealthCheckStatus.Status.OK, i18n.connectionSuccessful());
        } else {
            return new HealthCheckStatus(HealthCheckStatus.Status.KO, i18n.accessTokenRetrievalError(result.status(), ""));
        }
    }

    @AsyncValidation(URL_CHECK)
    public ValidationResult validateUrl(final String url) {
        try {
            new URL(url);
            return new ValidationResult(ValidationResult.Status.OK, null);
        } catch (MalformedURLException e) {
            return new ValidationResult(ValidationResult.Status.KO, e.getMessage());
        }
    }

    @Suggestions(LEAD_KEY_NAME_LIST)
    public SuggestionValues getLeadKeyNames(@Option final MarketoInputDataSet dataSet) {
        LOG.warn("[getLeadKeyNames] {}", dataSet);
        return new SuggestionValues(true, Arrays.asList( //
                new SuggestionValues.Item("id", "id"), //
                new SuggestionValues.Item("cookie", "cookie"), //
                new SuggestionValues.Item("email", "email"), //
                new SuggestionValues.Item("twitterId", "twitterId"), //
                new SuggestionValues.Item("facebookId", "facebookId"), //
                new SuggestionValues.Item("linkedInId", "linkedInId"), //
                new SuggestionValues.Item("sfdcAccountId", "sfdcAccountId"), //
                new SuggestionValues.Item("sfdcContactId", "sfdcContactId"), //
                new SuggestionValues.Item("sfdcLeadId", "sfdcLeadId"), //
                new SuggestionValues.Item("sfdcLeadOwnerId", "sfdcLeadOwnerId"), //
                new SuggestionValues.Item("sfdcOpptyId", "sfdcOpptyId"), //
                new SuggestionValues.Item("Custom", "Custom") //
        ));
    }

    @Suggestions(ACTIVITIES_LIST)
    public SuggestionValues getActivities(@Option final MarketoDataStore dataStore) {
        LOG.warn("[getActivities] {}.", dataStore);
        try {
            initClients(dataStore);
            String aToken = authorizationClient.getAccessToken(dataStore);
            List<Item> activities = new ArrayList<>();
            for (JsonObject act : parseResultFromResponse(leadClient.getActivities(aToken)).getValuesAs(JsonObject.class)) {
                activities.add(new SuggestionValues.Item(String.valueOf(act.getInt(ATTR_ID)), act.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, activities);
        } catch (Exception e) {
            LOG.warn("[getActivities] Exception: {}", e.getMessage());
            return new SuggestionValues(true, Arrays.asList( //
                    new SuggestionValues.Item("1", "Visit Webpage"), //
                    new SuggestionValues.Item("2", "Fill Out Form"), //
                    new SuggestionValues.Item("3", "Click Link"), //
                    new SuggestionValues.Item("6", "Send Email"), //
                    new SuggestionValues.Item("7", "Email Delivered"), //
                    new SuggestionValues.Item("8", "Email Bounced"), //
                    new SuggestionValues.Item("9", "Unsubscribe Email"), //
                    new SuggestionValues.Item("10", "Open Email"), //
                    new SuggestionValues.Item("11", "Click Email"), //
                    new SuggestionValues.Item("12", "New Lead"), //
                    new SuggestionValues.Item("13", "Change Data Value"), //
                    new SuggestionValues.Item("19", "Sync Lead to SFDC"), //
                    new SuggestionValues.Item("21", "Convert Lead"), //
                    new SuggestionValues.Item("22", "Change Score"), //
                    new SuggestionValues.Item("23", "Change Owner"), //
                    new SuggestionValues.Item("24", "Add to List"), //
                    new SuggestionValues.Item("25", "Remove from List"), //
                    new SuggestionValues.Item("26", "SFDC Activity"), //
                    new SuggestionValues.Item("27", "Email Bounced Soft"), //
                    new SuggestionValues.Item("29", "Delete Lead from SFDC"), //
                    new SuggestionValues.Item("30", "SFDC Activity Updated"), //
                    new SuggestionValues.Item("32", "Merge Leads"), //
                    new SuggestionValues.Item("34", "Add to Opportunity"), //
                    new SuggestionValues.Item("35", "Remove from Opportunity"), //
                    new SuggestionValues.Item("36", "Update Opportunity"), //
                    new SuggestionValues.Item("37", "Delete Lead"), //
                    new SuggestionValues.Item("38", "Send Alert"), //
                    new SuggestionValues.Item("39", "Send Sales Email"), //
                    new SuggestionValues.Item("40", "Open Sales Email"), //
                    new SuggestionValues.Item("41", "Click Sales Email"), //
                    new SuggestionValues.Item("42", "Add to SFDC Campaign"), //
                    new SuggestionValues.Item("43", "Remove from SFDC Campaign"), //
                    new SuggestionValues.Item("44", "Change Status in SFDC Campaign"), //
                    new SuggestionValues.Item("45", "Receive Sales Email"), //
                    new SuggestionValues.Item("46", "Interesting Moment"), //
                    new SuggestionValues.Item("47", "Request Campaign"), //
                    new SuggestionValues.Item("48", "Sales Email Bounced"), //
                    new SuggestionValues.Item("100", "Change Lead Partition"), //
                    new SuggestionValues.Item("101", "Change Revenue Stage"), //
                    new SuggestionValues.Item("102", "Change Revenue Stage Manually"), //
                    new SuggestionValues.Item("104", "Change Status in Progression"), //
                    new SuggestionValues.Item("106", "Enrich with Data.com"), //
                    new SuggestionValues.Item("108", "Change Segment"), //
                    new SuggestionValues.Item("110", "Call Webhook"), //
                    new SuggestionValues.Item("111", "Sent Forward to Friend Email"), //
                    new SuggestionValues.Item("112", "Received Forward to Friend Email"), //
                    new SuggestionValues.Item("113", "Add to Nurture"), //
                    new SuggestionValues.Item("114", "Change Nurture Track"), //
                    new SuggestionValues.Item("115", "Change Nurture Cadence"), //
                    new SuggestionValues.Item("145", "Push Lead to Marketo"), //
                    new SuggestionValues.Item("400", "Share Content"), //
                    new SuggestionValues.Item("401", "Vote in Poll"), //
                    new SuggestionValues.Item("402", "Sign Up for Referral Offer"), //
                    new SuggestionValues.Item("403", "Achieve Goal in Referral"), //
                    new SuggestionValues.Item("405", "Click Shared Link"), //
                    new SuggestionValues.Item("406", "Win Sweepstakes"), //
                    new SuggestionValues.Item("407", "Enter Sweepstakes"), //
                    new SuggestionValues.Item("408", "Disqualify Sweepstakes"), //
                    new SuggestionValues.Item("409", "Earn Entry in Social App"), //
                    new SuggestionValues.Item("410", "Refer to Social App") //
            ));
        }
    }

    @Suggestions(LIST_NAMES)
    public SuggestionValues getListNames(@Option final MarketoDataStore dataStore) {
        LOG.warn("[getListNames] {}.", dataStore);
        try {
            initClients(dataStore);
            String aToken = authorizationClient.getAccessToken(dataStore);
            List<Item> listNames = new ArrayList<>();
            for (JsonObject l : parseResultFromResponse(listClient.getLists(aToken, null, null, "", "", "", 300))
                    .getValuesAs(JsonObject.class)) {
                listNames.add(new SuggestionValues.Item(String.valueOf(l.getString(ATTR_NAME)), l.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, listNames);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Suggestions(FIELD_NAMES)
    public SuggestionValues getFieldNames(@Option final MarketoDataStore dataStore, @Option final String entity,
            @Option final String customObjectName) {
        LOG.warn("[getFieldNames] {}.", dataStore, entity, customObjectName);
        try {
            List<Item> fieldNames = new ArrayList<>();
            Schema schema = getEntitySchema(dataStore, entity, customObjectName, "");
            for (Entry f : schema.getEntries()) {
                fieldNames.add(new SuggestionValues.Item(f.getName(), f.getName()));
            }
            return new SuggestionValues(false, fieldNames);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @DynamicValues(FIELD_NAME_PROPOSITIONS)
    public Values getFieldNamePropositions() {
        MarketoDataStore dataStore = null;
        String entity = "", customObjectName = "";
        LOG.warn("[getFieldNames] {}.", dataStore, entity, customObjectName);
        try {
            List<Values.Item> fieldNames = new ArrayList<>();
            Schema schema = getEntitySchema(dataStore, entity, customObjectName, "");
            for (Entry f : schema.getEntries()) {
                fieldNames.add(new Values.Item(f.getName(), f.getName()));
            }

            return new Values(fieldNames);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @Suggestions(CUSTOM_OBJECT_NAMES)
    public SuggestionValues getCustomObjectNames(@Option final MarketoDataStore dataStore) {
        LOG.warn("[getCustomObjectNames] {}.", dataStore);
        try {
            initClients(dataStore);
            String aToken = authorizationClient.getAccessToken(dataStore);
            List<Item> coNames = new ArrayList<>();
            for (JsonObject l : parseResultFromResponse(customObjectClient.listCustomObjects(aToken, ""))
                    .getValuesAs(JsonObject.class)) {
                coNames.add(new SuggestionValues.Item(String.valueOf(l.getString(ATTR_NAME)), l.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, coNames);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
    }

    @DiscoverSchema(GUESS_ENTITY_SCHEMA_INPUT)
    public Schema guessEntitySchemaForInput(@Option(MarketoInputDataSet.NAME) final MarketoInputDataSet dataSet) {
        LOG.warn("[guessEntitySchemaForInput] {}", dataSet);
        MarketoEntity entity = dataSet.getEntity();
        return getEntitySchema(dataSet.getDataStore(), dataSet.getEntity().name(), dataSet.getCustomObjectName(),
                dataSet.getListAction().name());
    }

    public Schema getEntitySchema(final MarketoDataStore dataStore, final String entity, final String customObjectName,
            final String listAction) {
        LOG.warn("[getEntitySchema] {} - {} - {}- {}", dataStore, entity, customObjectName, listAction);
        try {
            initClients(dataStore);
            String accessToken = authorizationClient.getAccessToken(dataStore);
            JsonArray entitySchema = null;
            switch (MarketoEntity.valueOf(entity)) {
            case Lead:
                entitySchema = parseResultFromResponse(leadClient.describeLead(accessToken));
                break;
            case List:
                if (ListAction.getLeads.name().equals(listAction)) {
                    entitySchema = parseResultFromResponse(leadClient.describeLead(accessToken));
                } else {
                    return getInputSchema(MarketoEntity.List, listAction);
                }
                break;
            case CustomObject:
                entitySchema = parseResultFromResponse(customObjectClient.describeCustomObjects(accessToken, customObjectName))
                        .get(0).asJsonObject().getJsonArray(ATTR_FIELDS);
                break;
            case Company:
                entitySchema = parseResultFromResponse(companyClient.describeCompanies(accessToken)).get(0).asJsonObject()
                        .getJsonArray(ATTR_FIELDS);
                break;
            case Opportunity:
                entitySchema = parseResultFromResponse(opportunityClient.describeOpportunity(accessToken)).get(0).asJsonObject()
                        .getJsonArray(ATTR_FIELDS);
                break;
            case OpportunityRole:
                entitySchema = parseResultFromResponse(opportunityClient.describeOpportunityRole(accessToken)).get(0)
                        .asJsonObject().getJsonArray(ATTR_FIELDS);
                break;
            }
            LOG.warn("[guessEntitySchema]entitySchema: {}.", entitySchema);
            return getSchemaForEntity(entitySchema);
        } catch (Exception e) {
            LOG.error("Exception caught : {}.", e.getMessage());
        }
        return null;
    }

    @DiscoverSchema(GUESS_ENTITY_SCHEMA_OUTPUT)
    public Schema guessEntitySchemaForOutput(@Option final MarketoOutputDataSet dataSet) {
        LOG.warn("[guessEntitySchemaForOutput] {}", dataSet);
        return getOutputSchema(dataSet.getEntity());
    }

}
