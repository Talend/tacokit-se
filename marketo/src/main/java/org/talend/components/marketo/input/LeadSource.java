// ============================================================================
//
// Copyright (C) 2006-2019 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.input;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.EnumMap;
import java.util.function.Supplier;

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
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED;
import static org.talend.components.marketo.MarketoApiConstants.REQUEST_PARAM_QUERY_METHOD_GET;

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
        action.put(MarketoDataSet.LeadAction.getLeadChanges, this::getLeadChanges);
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
        String fields = configuration.getDataSet().getFields() == null ? null
                : configuration.getDataSet().getFields().stream().collect(joining(","));
        return handleResponse(listClient.getLeadsByListId(accessToken, nextPageToken, listId, fields));
    }

    private JsonObject getLead() {
        Integer leadId = configuration.getLeadId();
        String fields = configuration.getDataSet().getFields() == null ? null
                : configuration.getDataSet().getFields().stream().collect(joining(","));
        return handleResponse(leadClient.getLeadById(accessToken, leadId, fields));
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

    private JsonObject getMultipleLeads() {
        String filterType = configuration.getLeadKeyName();
        String filterValues = configuration.getLeadKeyValues();
        String fields = configuration.getDataSet().getFields() == null ? null
                : configuration.getDataSet().getFields().stream().collect(joining(","));
        if (isLeadUrlSizeGreaterThan8k(filterType, filterValues, fields)) {
            return handleResponse(leadClient.getLeadByFilterType(HEADER_CONTENT_TYPE_APPLICATION_X_WWW_FORM_URLENCODED,
                    REQUEST_PARAM_QUERY_METHOD_GET, accessToken, nextPageToken, buildLeadForm(filterType, filterValues, fields)));
        } else {
            return handleResponse(
                    leadClient.getLeadByFilterTypeByQueryString(accessToken, filterType, filterValues, fields, nextPageToken));
        }
    }

    private String computeDateTimeFromConfiguration() {
        log.warn("[computeDateTimeFromConfiguration] SinceDateTime [{}] : {}.", configuration.getDataSet().getDateTimeMode(),
                configuration.getDataSet().getSinceDateTime());
        String result;
        if (DateTimeMode.absolute.equals(configuration.getDataSet().getDateTimeMode())) {
            result = configuration.getDataSet().getSinceDateTime();
        } else {
            result = ZonedDateTime.now().minusDays(Integer.parseInt(configuration.getDataSet().getSinceDateTime()))
                    .format(DateTimeFormatter.ofPattern(DATETIME_FORMAT));
        }
        log.warn("[computeDateTimeFromConfiguration] SinceDateTime = {}.", result);
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
        String assetIds = configuration.getAssetIds();
        String listId = configuration.getDataSet().getListId();
        String leadIds = configuration.getLeadIds();
        return handleResponse(
                leadClient.getLeadActivities(accessToken, nextPageToken, activityTypeIds, assetIds, listId, leadIds));
    }

    /**
     * Returns a list of Data Value Changes and New Lead activities after a given datetime.
     */
    private JsonObject getLeadChanges() {
        if (nextPageToken == null) {
            nextPageToken = getPagingToken(computeDateTimeFromConfiguration());
        }
        String listId = configuration.getDataSet().getListId();
        String leadIds = configuration.getLeadIds();
        String fields = configuration.getDataSet().getFields() == null ? null
                : configuration.getDataSet().getFields().stream().collect(joining(","));
        return handleResponse(leadClient.getLeadChanges(accessToken, nextPageToken, listId, leadIds, fields));
    }

    public JsonObject getActivities() {
        return handleResponse(leadClient.getActivities(accessToken));
    }

    public String getPagingToken(String dateTime) {
        return handleResponse(leadClient.getPagingToken(accessToken, dateTime)).getString(ATTR_NEXT_PAGE_TOKEN);
    }

}
