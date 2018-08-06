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

import static org.talend.components.marketo.MarketoApiConstants.ATTR_ACCESS_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_BATCH_SIZE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FIELDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FILTER_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_FILTER_VALUES;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LEAD_IDS;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.METHOD_POST;
import static org.talend.components.marketo.MarketoApiConstants.REQUEST_PARAM_QUERY_METHOD;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

/**
 * Client for managing Leads
 *
 * 
 */
public interface LeadClient extends HttpClient {

    /**
     * Returns metadata about lead objects in the target instance, including a list of all fields available for interaction
     * via the APIs.
     * 
     * @param accessToken Marketo authorization token for API
     * @return
     */
    @Request(path = "/rest/v1/leads/describe.json")
    Response<JsonObject> describeLead(@Query(ATTR_ACCESS_TOKEN) String accessToken);

    /**
     * Retrieves a single lead record through it's Marketo id.
     * 
     * @param accessToken
     * @return
     */
    @Request(path = "/rest/v1/lead/{leadId}.json")
    Response<JsonObject> getLeadById( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_LEAD_ID) Integer leadId, //
            @Query(ATTR_FIELDS) String fields//
    );

    /**
     * Returns a list of up to 300 leads based on a list of values in a particular field.
     *
     * @param accessToken Marketo authorization token for API
     * @param filterType The lead field to filter on. Custom fields (string, email, integer), and the following field types
     * are supported: id, cookies, email, twitterId, facebookId, linkedInId, sfdcAccountId, sfdcContactId, sfdcLeadId,
     * sfdcLeadOwnerId, sfdcOpptyId.
     * @param filterValues A comma-separated list of values to filter on in the specified fields.
     * @param fields A comma-separated list of lead fields to return for each record
     * @param batchSize The batch size to return. The max and default value is 300.
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter
     * @return
     */
    @Request(path = "/rest/v1/leads.json")
    Response<JsonObject> getLeadByFilterTypeByQueryString( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_FILTER_TYPE) String filterType, //
            @Query(ATTR_FILTER_VALUES) String filterValues, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken// .
    );

    /**
     * Returns a list of up to 300 leads based on a list of values in a particular field.
     *
     * This method is used when the query string size is over 8kb.
     * 
     * @param accessToken Marketo authorization token for API
     * @param payload is a string for application/x-www-form-urlencoded containing the following parameters
     * <ul>
     * <li>@param filterType The lead field to filter on. Custom fields (string, email, integer), and the following field
     * types are supported: id, cookies, email, twitterId, facebookId, linkedInId, sfdcAccountId, sfdcContactId, sfdcLeadId,
     * sfdcLeadOwnerId, sfdcOpptyId.</li>
     * <li>@param filterValues A comma-separated list of values to filter on in the specified fields.</li>
     * <li>@param fields A comma-separated list of lead fields to return for each record</li>
     * <li>@param batchSize The batch size to return. The max and default value is 300.</li>
     * </ul>
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter
     * @return
     */
    @Request(path = "/rest/v1/leads.json", method = METHOD_POST)
    Response<JsonObject> getLeadByFilterType( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(REQUEST_PARAM_QUERY_METHOD) String queryMethod, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken, //
            String payload //
    );

    /**
     * Syncs a list of leads to the target instance.
     * 
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param action Type of sync operation to perform. Defaults to createOrUpdate if unset = ['createOnly',
     * 'updateOnly', 'createOrUpdate', 'createDuplicate'],</li>
     * <li>@param lookupField Field to deduplicate on. The field must be present in each lead record of the input. Defaults
     * to email if unset ,</li>
     * <li>@param input List of leads for input</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/leads.json", method = METHOD_POST)
    Response<JsonObject> syncLeads( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /**
     * Delete a list of leads from the destination instance.
     * 
     * @param accessToken Marketo authorization token for API
     * @param input List of leads for input
     * @return
     */
    @Request(path = "/rest/v1/leads/delete.json", method = METHOD_POST)
    Response<JsonObject> deleteLeads( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject input //
    );

    /**
     * Returns a paging token for use in retrieving activities and data value changes.
     * 
     * @param accessToken Marketo authorization token for API
     * @param sinceDatetime Earliest datetime to retrieve activities from
     * @return
     */
    @Request(path = "/rest/v1/activities/pagingtoken.json")
    Response<JsonObject> getPagingToken( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query("sinceDatetime") String sinceDatetime //
    );

    /**
     * Returns a list of Data Value Changes and New Lead activities after a given datetime.
     * 
     * @param accessToken Marketo authorization token for API
     * @param nextPageToken (X) Token representation of a datetime returned by the Get Paging Token endpoint. This endpoint
     * will return activities after this datetime
     * @param fields (X) Comma-separated list of field names to return changes for. Field names can be retrieved with the
     * Describe Lead API.
     * @param listId Id of a static list. If set, will only return activities of members of this static list.
     * @param leadIds Comma-separated list of lead ids. If set, will only return activities of the leads with these ids.
     * Allows up to 30 entries.
     * @param batchSize Maximum number of records to return. Maximum and default is 300.
     * @return
     */
    @Request(path = "/rest/v1/activities/leadchanges.json")
    Response<JsonObject> getLeadChanges( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken, //
            @Query(ATTR_LIST_ID) Integer listId, //
            @Query(ATTR_LEAD_IDS) String leadIds, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize //
    );

    /**
     * Returns a list of available activity types in the target instance, along with associated metadata of each type.
     * 
     * @param accessToken accessToken Marketo authorization token for API
     *
     * @return
     */
    @Request(path = "/rest/v1/activities/types.json")
    Response<JsonObject> getActivities( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken //
    );

    /**
     * Returns a list of activities from after a datetime given by the nextPageToken parameter. Also allows for filtering by
     * lead static list membership, or by a list of up to 30 lead ids.
     * 
     * @param accessToken Marketo authorization token for API
     * @param nextPageToken (X) Token representation of a datetime returned by the Get Paging Token endpoint. This endpoint
     * will return activities after this datetime
     * @param activityTypeIds (X) Comma-separated list of activity type ids. These can be retrieved with the Get Activity
     * Types API
     * @param assetIds Id of the primary asset for an activity. This is based on the primary asset id of a given activity
     * type. Should only be used when a single activity type is set
     * @param listId Id of a static list. If set, will only return activities of members of this static list.
     * @param leadIds Comma-separated list of lead ids. If set, will only return activities of the leads with these ids.
     * Allows up to 30 entries.
     * @param batchSize
     * @return
     */
    @Request(path = "/rest/v1/activities.json")
    Response<JsonObject> getLeadActivities( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken, //
            @Query("activityTypeIds") String activityTypeIds, //
            @Query("assetIds") String assetIds, //
            @Query(ATTR_LIST_ID) Integer listId, //
            @Query(ATTR_LEAD_IDS) String leadIds, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize //
    );

}
