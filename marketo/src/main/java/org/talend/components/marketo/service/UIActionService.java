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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoDataSet.MarketoEntity;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;
import org.talend.sdk.component.api.service.schema.DiscoverSchema;
import org.talend.sdk.component.api.service.schema.Schema;
import org.talend.sdk.component.api.service.schema.Schema.Entry;
import org.talend.sdk.component.api.service.schema.Type;

@Service
public class UIActionService extends MarketoService {

    public static final String ACTIVITIES_LIST = "ACTIVITIES_LIST";

    public static final String LEAD_KEY_NAME_LIST = "LEAD_KEY_NAME_LIST";

    public static final String HEALTH_CHECK = "MARKETO_HEALTH_CHECK";

    public static final String URL_CHECK = "MARKETO_URL_CHECK";

    public static final String GUESS_ENTITY_SCHEMA_INPUT = "guessEntitySchema";

    private transient static final Logger LOG = getLogger(UIActionService.class);

    @HealthCheck(HEALTH_CHECK)
    public HealthCheckStatus doHealthCheck(@Option(MarketoDataStore.NAME) MarketoDataStore dataStore, final I18nMessage i18n) {
        authorizationClient.base(dataStore.getEndpoint());
        Response<JsonObject> result = authorizationClient.getAuthorizationToken(CLIENT_CREDENTIALS, dataStore.getClientId(),
                dataStore.getClientSecret());
        if (result.status() == 200 && result.body().getString("access_token", null) != null) {
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
    public SuggestionValues getLeadKeyNames() {
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
    public SuggestionValues getActivities(@Option final MarketoInputDataSet dataSet) {
        LOG.warn("[getActivities] {}.", dataSet);
        try {
            String aToken = authorizationClient.getAccessToken(dataSet.getDataStore());
            leadClient.base(dataSet.getDataStore().getEndpoint());
            List<Item> activities = new ArrayList<>();
            for (JsonObject act : parseResultFromResponse(leadClient.getActivities(aToken)).getValuesAs(JsonObject.class)) {
                activities.add(new SuggestionValues.Item(String.valueOf(act.getInt(ATTR_ID)), act.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, activities);
        } catch (Exception e) {
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

    @DiscoverSchema(GUESS_ENTITY_SCHEMA_INPUT)
    public Schema guessEntitySchema(@Option final MarketoInputDataSet dataSet) {
        LOG.warn("[guessEntitySchema] {}.", dataSet);
        MarketoEntity entity = dataSet.getEntity();
        Schema s = new Schema();
        Collection<Entry> entries = new ArrayList<>();
        try {
            JsonArray entitySchema = null;
            String accessToken = authorizationClient.getAccessToken(dataSet.getDataStore());
            String endpoint = dataSet.getDataStore().getEndpoint();
            switch (entity) {
            case Lead:
                leadClient.base(endpoint);
                entitySchema = parseResultFromResponse(leadClient.describeLead(accessToken));
                break;
            case List:
                listClient.base(endpoint);
                //
                break;
            case CustomObject:
                customObjectClient.base(endpoint);
                entitySchema = parseResultFromResponse(
                        customObjectClient.describeCustomObjects(accessToken, dataSet.getCustomObjectName())).get(0)
                                .asJsonObject().getJsonArray("fields");
                break;
            case Company:
                companyClient.base(endpoint);
                entitySchema = parseResultFromResponse(companyClient.describeCompanies(accessToken));
                break;
            case Opportunity:
                opportunityClient.base(endpoint);
                entitySchema = parseResultFromResponse(opportunityClient.describeOpportunity(accessToken));
                break;
            case OpportunityRole:
                opportunityClient.base(endpoint);
                entitySchema = parseResultFromResponse(opportunityClient.describeOpportunityRole(accessToken));
                break;
            }

            LOG.warn("[guessEntitySchema]entitySchema: {}.", entitySchema);

            for (JsonObject field : entitySchema.getValuesAs(JsonObject.class)) {
                Entry entry = new Entry();
                if (field.getJsonObject("rest") != null) {
                    entry.setName(field.getJsonObject("rest").getString("name"));
                } else {
                    entry.setName(field.getString("name"));
                }
                String dataType = field.getString("dataType", "string");
                switch (dataType) {
                case ("string"):
                case ("text"):
                case ("phone"):
                case ("email"):
                case ("url"):
                case ("lead_function"):
                case ("reference"):
                    entry.setType(Type.STRING);
                    break;
                case ("integer"):
                    entry.setType(Type.INT);
                    break;
                case ("boolean"):
                    entry.setType(Type.BOOLEAN);
                    break;
                case ("float"):
                case ("currency"):
                    entry.setType(Type.DOUBLE);
                    break;
                case ("date"):
                case ("datetime"):
                    entry.setType(Type.STRING);
                    break;
                default:
                    LOG.warn("Non managed type : {}. for {}. Defaulting to String.", dataType, this);
                    entry.setType(Type.STRING);
                }
                entries.add(entry);
            }
        } catch (Exception e) {
            LOG.error("Exception c=uaght : {}.", e.getCause().toString());
            entries.add(new Entry("Exception", Type.STRING));
        }
        s.setEntries(entries);
        LOG.warn("[guessEntitySchema] returning schema : {}.", s);
        return s;
    }

}
