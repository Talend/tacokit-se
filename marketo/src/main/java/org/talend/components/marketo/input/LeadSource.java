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
package org.talend.components.marketo.input;

import java.time.Period;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.json.JsonObject;

import org.talend.components.marketo.dataset.MarketoDataSet;
import org.talend.components.marketo.dataset.MarketoDataSet.DateTimeMode;
import org.talend.components.marketo.dataset.MarketoDataSet.LeadAction;
import org.talend.components.marketo.dataset.MarketoInputConfiguration;
import org.talend.components.marketo.service.LeadClient;
import org.talend.components.marketo.service.ListClient;
import org.talend.components.marketo.service.MarketoService;
import org.talend.sdk.component.api.configuration.Option;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.joining;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FILTER_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FILTER_VALUES;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.DATETIME_FORMAT;

@Slf4j
public class LeadSource extends MarketoSource {

    private final LeadClient leadClient;

    private final ListClient listClient;

    private transient EnumMap<LeadAction, Supplier<JsonObject>> action = new EnumMap<>(LeadAction.class);

    public LeadSource(@Option("configuration") final MarketoInputConfiguration configuration, //
            final MarketoService service) {
        super(configuration, service);
        this.leadClient = service.getLeadClient();
        this.leadClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
        this.listClient = service.getListClient();
        this.listClient.base(this.configuration.getDataSet().getDataStore().getEndpoint());
        action.put(MarketoDataSet.LeadAction.getLeadActivity, this::getLeadActivities);
        action.put(MarketoDataSet.LeadAction.getLeadsByList, this::getLeadsByListId);
    }

    @Override
    public JsonObject runAction() {
        Supplier<JsonObject> meth = action.get(configuration.getDataSet().getLeadAction());
        if (meth == null) {
            throw new IllegalArgumentException(i18n.invalidOperation());
        }
        return meth.get();
    }

    private JsonObject getLeadsByListId() {
        Integer listId = Integer.parseInt(configuration.getDataSet().getListId());
        String fields = schema.keySet().stream().collect(Collectors.joining(","));
        return handleResponse(listClient.getLeadsByListId(accessToken, nextPageToken, listId, fields));
    }

    private Boolean isLeadUrlSizeGreaterThan8k(String filterType, String filterValues, String fields) {
        int pathSize = 20;
        int endpointSize = configuration.getDataSet().getDataStore().getEndpoint().length();
        int queryParameterNamesSize = ATTR_ACCESS_TOKEN.length() + 1 + (accessToken == null ? 0 : accessToken.length()) + //
                ATTR_NEXT_PAGE_TOKEN.length() + 1 + (nextPageToken == null ? 0 : nextPageToken.length()) + //
                endpointSize + //
                pathSize + //
                ATTR_ACCESS_TOKEN.length() + 1 + //
                ATTR_FILTER_TYPE.length() + 1 + //
                ATTR_FILTER_VALUES.length() + 1 + //
                ATTR_FIELDS.length() + 1; //
        int queryParameterValuesSize = (filterType == null ? 0 : filterType.length())
                + (filterValues == null ? 0 : filterValues.length()) + (fields == null ? 0 : fields.length());
        int total = queryParameterNamesSize + queryParameterValuesSize;
        return total >= (8 * 1024);
    }

    private String buildLeadForm(String filterType, String filterValues, String fields) {
        StringBuilder sb = new StringBuilder();
        sb.append(ATTR_FILTER_TYPE + "=" + filterType.trim());
        sb.append("&");
        sb.append(ATTR_FILTER_VALUES + "=" + filterValues.trim());
        sb.append("&");
        sb.append(ATTR_FIELDS + "=" + fields.trim());

        return sb.toString();
    }

    private String computeDateTimeFromConfiguration() {
        String result;
        if (DateTimeMode.absolute.equals(configuration.getDataSet().getDateTimeMode())) {
            result = configuration.getDataSet().getSinceDateTimeAbsolute();
        } else {
            result = ZonedDateTime.now()
                    .minus(Period.parse(configuration.getDataSet().getSinceDateTimeRelative().getRelativeOffset()))
                    .format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
        }

        return result;
    }

    /**
     * Returns a list of activities from after a datetime given by the nextPageToken parameter. Also allows for
     * filtering by lead
     * static list membership, or by a list of up to 30 lead ids.
     *
     * @return
     */
    private JsonObject getLeadActivities() {
        if (nextPageToken == null) {
            nextPageToken = getPagingToken(computeDateTimeFromConfiguration());
        }
        String activityTypeIds = "";
        if (!configuration.getDataSet().getActivityTypeIds().isEmpty()) {
            activityTypeIds = configuration.getDataSet().getActivityTypeIds().stream().collect(joining(","));
        }
        String listId = configuration.getDataSet().getListId();
        return handleResponse(leadClient.getLeadActivities(accessToken, nextPageToken, activityTypeIds, "", listId, ""));
    }

    public JsonObject getActivities() {
        return handleResponse(leadClient.getActivities(accessToken));
    }

    public String getPagingToken(String dateTime) {
        return handleResponse(leadClient.getPagingToken(accessToken, dateTime)).getString(ATTR_NEXT_PAGE_TOKEN);
    }

}
