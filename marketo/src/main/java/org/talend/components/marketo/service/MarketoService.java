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

import static java.util.stream.Collectors.joining;
import static org.slf4j.LoggerFactory.getLogger;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import javax.json.JsonArray;
import javax.json.JsonObject;

import org.slf4j.Logger;
import org.talend.components.marketo.dataset.MarketoInputDataSet;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;

@Service
public class MarketoService {

    @Service
    private AuthorizationClient authorizationClient;

    @Service
    private LeadClient leadClient;

    private I18nMessage i18n;

    private transient static final Logger LOG = getLogger(MarketoService.class);

    public static final String ACTIVITIES_LIST = "ACTIVITIES_LIST";

    @HealthCheck("marketo_healthcheck")
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

    @AsyncValidation("urlValidation")
    public ValidationResult validateUrl(final String url) {
        try {
            new URL(url);
            return new ValidationResult(ValidationResult.Status.OK, null);
        } catch (MalformedURLException e) {
            return new ValidationResult(ValidationResult.Status.KO, e.getMessage());
        }
    }

    public String getFieldsFromDescribeFormatedForApi(JsonArray fields) {
        List<String> result = new ArrayList<>();
        for (JsonObject field : fields.getValuesAs(JsonObject.class)) {
            if (field.getJsonObject("rest") != null) {
                result.add(field.getJsonObject("rest").getString("name"));
            } else {
                result.add(field.getString("name"));
            }
        }
        return result.stream().collect(joining(","));
    }

    public List<String> getLeadKeyNames() {
        return Arrays.asList("id", "cookie", "email", "twitterId", "facebookId", "linkedInId", "sfdcAccountId", "sfdcContactId",
                "sfdcLeadId", "sfdcLeadOwnerId", "sfdcOpptyId", "Custom");
    }

    @Suggestions(ACTIVITIES_LIST)
    public SuggestionValues getActivities(@Option final MarketoInputDataSet dataSet) {
        String aToken = authorizationClient.getAccessToken(dataSet.getDataStore());
        leadClient.base(dataSet.getDataStore().getEndpoint());
        Response<JsonObject> response = leadClient.getActivities(aToken);
        if (response.status() == 200 && response.body() != null && response.body().getJsonArray(ATTR_RESULT) != null) {
            List<SuggestionValues.Item> activities = new ArrayList<>();
            for (JsonObject act : response.body().getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class)) {
                activities.add(new SuggestionValues.Item(String.valueOf(act.getInt(ATTR_ID)), act.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, activities);
        }
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
