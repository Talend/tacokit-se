/*
 * Copyright (C) 2006-2021 Talend Inc. - www.talend.com
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
package org.talend.components.marketo.service;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;

import org.talend.components.marketo.MarketoApiConstants;
import org.talend.components.marketo.MarketoRuntimeException;
import org.talend.components.marketo.datastore.MarketoDataStore;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.record.Schema;
import org.talend.sdk.component.api.record.Schema.Entry;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.asyncvalidation.AsyncValidation;
import org.talend.sdk.component.api.service.asyncvalidation.ValidationResult;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;
import org.talend.sdk.component.api.service.healthcheck.HealthCheck;
import org.talend.sdk.component.api.service.healthcheck.HealthCheckStatus;
import org.talend.sdk.component.api.service.http.Response;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_RESULT;
import static org.talend.components.marketo.service.AuthorizationClient.CLIENT_CREDENTIALS;

@Slf4j
@Service
public class UIActionService extends MarketoService {

    public static final String ACTIVITIES_LIST = "ACTIVITIES_LIST";

    public static final String LEAD_KEY_NAME_LIST = "LEAD_KEY_NAME_LIST";

    public static final String DATE_RANGES = "DATE_RANGES";

    public static final String LIST_NAMES = "LIST_NAMES";

    public static final String FIELD_NAMES = "FIELD_NAMES";

    public static final String CUSTOM_OBJECT_NAMES = "CUSTOM_OBJECT_NAMES";

    public static final String HEALTH_CHECK = "MARKETO_HEALTH_CHECK";

    public static final String VALIDATION_URL_PROPERTY = "MARKETO_VALIDATION_URL_PROPERTY";

    public static final String VALIDATION_STRING_PROPERTY = "VALIDATION_STRING_PROPERTY";

    public static final String VALIDATION_INTEGER_PROPERTY = "VALIDATION_INTEGER_PROPERTY";

    public static final String VALIDATION_LIST_PROPERTY = "VALIDATION_LIST_PROPERTY";

    public static final String VALIDATION_DATETIME_PATTERN = "VALIDATION_DATETIME_PATTERN";

    public static final String DATETIME_PATTERN = "\\d{4}-[01]\\d-[0-3]\\d [0-2]\\d:[0-5]\\d:[0-5]\\d";

    public static final String DATE_MODE_RELATIVE = "relative";

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

    @Suggestions(LEAD_KEY_NAME_LIST)
    public SuggestionValues suggestLeadKeyNames(@Option final MarketoDataStore dataStore) {
        try {
            initClients(dataStore);
            JsonArray sf = parseResultFromResponse(leadClient.describeLead2(authorizationClient.getAccessToken(dataStore)));
            List<String> f = sf.getJsonObject(0).getJsonArray(MarketoApiConstants.ATTR_SEARCHABLE_FIELDS).stream()
                    .map(JsonValue::asJsonArray).map(e -> e.getString(0)).sorted().collect(Collectors.toList());
            List<Item> fieldNames = new ArrayList<>();
            for (String fn : f) {
                fieldNames.add(new SuggestionValues.Item(fn, fn));
            }
            return new SuggestionValues(false, fieldNames);
        } catch (Exception e) {
            log.warn("[suggestLeadKeyNames] {}", e.getMessage());
            return new SuggestionValues(true, Arrays.asList( //
                    new SuggestionValues.Item("id", "id"), //
                    new SuggestionValues.Item("cookies", "cookies"), //
                    new SuggestionValues.Item("email", "email"), //
                    new SuggestionValues.Item("twitterId", "twitterId"), //
                    new SuggestionValues.Item("facebookId", "facebookId"), //
                    new SuggestionValues.Item("linkedInId", "linkedInId"), //
                    new SuggestionValues.Item("sfdcAccountId", "sfdcAccountId"), //
                    new SuggestionValues.Item("sfdcContactId", "sfdcContactId"), //
                    new SuggestionValues.Item("sfdcLeadId", "sfdcLeadId"), //
                    new SuggestionValues.Item("sfdcLeadOwnerId", "sfdcLeadOwnerId"), //
                    new SuggestionValues.Item("sfdcOpptyId", "sfdcOpptyId"), //
                    new SuggestionValues.Item("leadPartitionId", "leadPartitionId") //
            ));
        }
    }

    @Suggestions(ACTIVITIES_LIST)
    public SuggestionValues getActivities(@Option final MarketoDataStore dataStore) {
        log.debug("[getActivities] {}.", dataStore);
        try {
            initClients(dataStore);
            String aToken = authorizationClient.getAccessToken(dataStore);
            List<Item> activities = new ArrayList<>();
            for (JsonObject act : parseResultFromResponse(leadClient.getActivities(aToken)).getValuesAs(JsonObject.class)) {
                activities.add(new SuggestionValues.Item(String.valueOf(act.getInt(ATTR_ID)), act.getString(ATTR_NAME)));
            }
            return new SuggestionValues(true, activities);
        } catch (Exception e) {
            log.warn("[getActivities] Exception: {}", e.getMessage());
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

    @AllArgsConstructor
    @Getter
    private class ListsPage {

        String nextPageToken;

        List<Item> lists;

    }

    private ListsPage getListsPage(String token, String nextPage) {
        List<Item> lists = new ArrayList<>();
        Consumer<JsonObject> listConsumer = l -> lists.add(new Item(String.valueOf(l.getInt(ATTR_ID)), l.getString(ATTR_NAME)));
        JsonObject result = handleResponse(listClient.getLists(token, nextPage, null, "", "", ""));
        String nextPageToken = result.getString(ATTR_NEXT_PAGE_TOKEN, null);
        if (result.getJsonArray(ATTR_RESULT) != null) {
            result.getJsonArray(ATTR_RESULT).getValuesAs(JsonObject.class).forEach(listConsumer);
        }
        return new ListsPage(nextPageToken, lists);
    }

    @Suggestions(LIST_NAMES)
    public SuggestionValues getListNames(@Option final MarketoDataStore dataStore) {
        log.debug("[getListNames] {}.", dataStore);
        List<Item> lists = new ArrayList<>();
        Predicate<String> hasNextPageToken = token -> token != null && !token.isEmpty();
        try {
            initClients(dataStore);
            String aToken = authorizationClient.getAccessToken(dataStore);
            ListsPage result = getListsPage(aToken, null);
            lists.addAll(result.getLists());
            while (hasNextPageToken.test(result.getNextPageToken())) {
                result = getListsPage(aToken, result.getNextPageToken());
                lists.addAll(result.getLists());
            }
            return new SuggestionValues(true, lists);
        } catch (Exception e) {
            throw new MarketoRuntimeException(e.getMessage());
        }
    }

    @Suggestions(FIELD_NAMES)
    public SuggestionValues getFieldNames(@Option final MarketoDataStore dataStore) {
        try {
            List<Item> fieldNames = new ArrayList<>();
            Schema schema = getEntitySchema(dataStore);
            for (Entry f : schema.getEntries()) {
                fieldNames.add(new SuggestionValues.Item(f.getName(), f.getName()));
            }
            return new SuggestionValues(false, fieldNames);
        } catch (Exception e) {
            throw new MarketoRuntimeException(e.getMessage());
        }
    }

    @Suggestions(CUSTOM_OBJECT_NAMES)
    public SuggestionValues getCustomObjectNames(@Option final MarketoDataStore dataStore) {
        log.debug("[getCustomObjectNames] {}.", dataStore);
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
            throw new MarketoRuntimeException(e.getMessage());
        }
    }

    @AsyncValidation(VALIDATION_URL_PROPERTY)
    public ValidationResult validateEndpoint(final String url) {
        try {
            new URL(url);
            return new ValidationResult(ValidationResult.Status.OK, null);
        } catch (MalformedURLException e) {
            return new ValidationResult(ValidationResult.Status.KO, e.getMessage());
        }
    }

    @AsyncValidation(VALIDATION_STRING_PROPERTY)
    public ValidationResult validateString(final String string) {
        if (string == null || string.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.invalidBlankProperty());
        }
        return new ValidationResult(ValidationResult.Status.OK, null);
    }

    @AsyncValidation(VALIDATION_INTEGER_PROPERTY)
    public ValidationResult validateInteger(final Integer integer) {
        if (integer == null || integer.toString().isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.invalidBlankProperty());
        }
        return new ValidationResult(ValidationResult.Status.OK, null);
    }

    @AsyncValidation(VALIDATION_LIST_PROPERTY)
    public ValidationResult validateList(final List<String> list) {
        if (list == null || list.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.invalidFields());
        }
        return new ValidationResult(ValidationResult.Status.OK, null);
    }

    @AsyncValidation(VALIDATION_DATETIME_PATTERN)
    public ValidationResult validateDateTimePattern(final String date) {
        log.debug("[validateDateTimePattern] {}", date);
        if (date == null || date.isEmpty()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.invalidBlankProperty());
        }
        Pattern pattern = Pattern.compile(DATETIME_PATTERN);
        if (!pattern.matcher(date).matches()) {
            return new ValidationResult(ValidationResult.Status.KO, i18n.invalidDateTime());
        }
        return new ValidationResult(ValidationResult.Status.OK, null);
    }

}
