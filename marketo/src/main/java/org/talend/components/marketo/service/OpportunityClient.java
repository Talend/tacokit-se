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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.METHOD_POST;
import static org.talend.components.marketo.MarketoApiConstants.REQUEST_PARAM_QUERY_METHOD;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

/**
 * Client for managing Opportunities and Opportunity Roles.
 *
 * Marketo exposes APIs for reading, writing, creating and updating opportunity records. In Marketo, opportunity records
 * are linked to lead and contact records through the intermediate Opportunity Role object, so an opportunity may be
 * linked to many individual leads. Both of these object types are exposed through the API, and like most of the Lead
 * Database object types, they both have a corresponding Describe call, which returns metadata about the object types.
 *
 * <b>Opportunity (Role) APIs are only exposed for subscriptions which do not have a native CRM sync enabled.</b>
 *
 */
public interface OpportunityClient extends HttpClient {
    /*
     *****************************************************************
     **** Opportunities
     ******************************************************************
     */

    /**
     * Returns metadata about opportunities and the fields available for interaction via the API.
     *
     * @param accessToken Marketo authorization token for API
     * @return metadata about opportunities
     */
    @Request(path = "/rest/v1/opportunities/describe.json")
    Response<JsonObject> describeOpportunity(@Query(ATTR_ACCESS_TOKEN) String accessToken);

    /**
     * Retrieves opportunity records from the destination instance based on the submitted filter.
     *
     * @param accessToken Marketo authorization token for API
     * @param filterType The Opportunities field to filter on. Searchable fields can be retrieved with the Describe call
     * @param filterValues Comma-separated list of values to match against query
     * @param fields Comma-separated list of fields to include in the response query
     * @param batchSize The batch size to return. The max and default value is 300.
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter
     * @return opportunity records
     */
    @Request(path = "/rest/v1/opportunities.json")
    Response<JsonObject> getOpportunities(@Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_FILTER_TYPE) String filterType, //
            @Query(ATTR_FILTER_VALUES) String filterValues, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken //
    );

    /**
     * Allows inserting, updating, or upserting of opportunity records into the target instance.
     *
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param action Type of sync operation to perform = ['createOnly', 'updateOnly', 'createOrUpdate']</li>
     * <li>@param dedupeBy Field to deduplicate on. If the value in the field for a given record is not unique, an error
     * will be returned for the individual record</li>
     * <li>@param input List of input records</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/opportunities.json", method = METHOD_POST)
    Response<JsonObject> syncOpportunities( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /**
     * Deletes a list of opportunity records from the target instance. Input records should only have one member, based on
     * the value of 'dedupeBy'.
     *
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param deleteBy Field to delete records by. Permissible values are idField or dedupeFields as indicated by the
     * result of the corresponding describe record.</li>
     * <li>@param input List of input records value</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/opportunities/delete.json", method = METHOD_POST)
    Response<JsonObject> deleteOpportunities( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /*
     *****************************************************************
     **** Opportunity Roles
     ******************************************************************
     */

    /**
     * Returns object and field metadata for Opportunity Roles in the target instance.
     *
     * @param accessToken Marketo authorization token for API
     * @return metadata about opportunity roles
     */
    @Request(path = "/rest/v1/opportunities/roles/describe.json")
    Response<JsonObject> describeOpportunityRole(@Query(ATTR_ACCESS_TOKEN) String accessToken);

    /**
     * Returns a list of opportunity roles based on a filter and set of values.
     *
     * @param accessToken Marketo authorization token for API
     * @param filterType The OpportunityRoles field to filter on. Searchable fields can be retrieved with the Describe call
     * @param filterValues Comma-separated list of values to match against query
     * @param fields Comma-separated list of fields to include in the response query
     * @param batchSize The batch size to return. The max and default value is 300.
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter
     * @return opportunity records
     */
    @Request(path = "/rest/v1/opportunities/roles.json")
    Response<JsonObject> getOpportunityRoles( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_FILTER_TYPE) String filterType, //
            @Query(ATTR_FILTER_VALUES) String filterValues, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken //
    );

    @Request(path = "/rest/v1/opportunities/roles.json", method = METHOD_POST)
    Response<JsonObject> getOpportunityRolesWithCompoundKey( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(REQUEST_PARAM_QUERY_METHOD) String queryMethod, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /**
     * Allows inserts, updates and upserts of Opportunity Role records in the target instance.
     *
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param action Type of sync operation to perform = ['createOnly', 'updateOnly', 'createOrUpdate']</li>
     * <li>@param dedupeBy Field to deduplicate on. If the value in the field for a given record is not unique, an error
     * will be returned for the individual record</li>
     * <li>@param input List of input records</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/opportunities/roles.json", method = METHOD_POST)
    Response<JsonObject> syncOpportunityRoles( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /**
     * Deletes a list of opportunities from the target instance.
     *
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param deleteBy Field to delete records by. Permissible values are idField or dedupeFields as indicated by the
     * result of the corresponding describe record.</li>
     * <li>@param input List of input records value</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/opportunities/roles/delete.json", method = METHOD_POST)
    Response<JsonObject> deleteOpportunityRoles( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

}
