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

import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

/**
 * Client for mangaging CustomObjects
 *
 * Marketo allows users to define custom objects which are related either to lead records, or account records.
 *
 * <b>Custom Objects are unavailable for some Marketo subscription types.</b>
 *
 */
public interface CustomObjectClient extends HttpClient {

    /**
     * Returns a list of Custom Object types available in the target instance, along with id and deduplication information
     * for each type.
     * 
     * @param accessToken Marketo authorization token for API.
     * @param names Comma-separated list of names to filter types on.
     * @return
     */
    @Request(path = "/rest/v1/customobjects.json", method = "GET")
    Response<JsonObject> listCustomObjects( //
            @Query("access_token") String accessToken, //
            @Query("names") String names //
    );

    /**
     * Returns metadata regarding a given custom object.
     *
     * @param accessToken Marketo authorization token for API.
     * @param customObjectName custom Object Name.
     * @return
     */
    @Request(path = "/rest/v1/customobjects/{customObjectName}/describe.json", method = "GET")
    Response<JsonObject> describeCustomObjects( //
            @Query("access_token") String accessToken, //
            @Path("customObjectName") String customObjectName //
    );

    // TODO should normally execute a fake get request when using Compound Key
    /**
     * Retrieves a list of custom objects records based on filter and set of values. When action is createOnly, idField may
     * not be used as a key and marketoGUID cannot be a member of any object records.
     * 
     * @param accessToken Marketo authorization token for API.
     * @param customObjectName custom Object Name.
     * @param filterType Field to filter on. Searchable fields can be retrieved with Describe Custom Object
     * @param filterValues Comma-separated list of field values to match against.
     * @param fields Comma-separated list of fields to return for each record. If unset marketoGuid, dedupeFields,
     * updatedAt, createdAt will be returned.
     * @param batchSize The batch size to return. The max and default value is 300.
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter.
     * @return
     */
    @Request(path = "/rest/v1/customobjects/{customObjectName}.json", method = "GET")
    Response<JsonObject> getCustomObjects( //
            @Query("access_token") String accessToken, //
            @Path("customObjectName") String customObjectName, //
            @Query("filterType") String filterType, //
            @Query("filterValues") String filterValues, //
            @Query("fields") String fields, //
            @Query("batchSize") Integer batchSize, //
            @Query("nextPageToken") String nextPageToken //
    );

    /**
     * Inserts, updates, or upserts custom object records to the target instance.
     * 
     * @param accessToken Marketo authorization token for API.
     * @param customObjectName custom Object Name.
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param action Type of sync operation to perform = ['createOnly', 'updateOnly', 'createOrUpdate'].</li>
     * <li>@param dedupeBy Field to deduplicate on. If the value in the field for a given record is not unique, an error
     * will be returned for the individual record.</li>
     * <li>@param input List of input records.</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/customobjects/{customObjectName}.json", method = "POST")
    Response<JsonObject> syncCustomObjects( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query("access_token") String accessToken, //
            @Path("customObjectName") String customObjectName, //
            JsonObject payload//
    );

    /**
     * Deletes a given set of custom object records.
     *
     * @param accessToken Marketo authorization token for API.
     * @param customObjectName custom Object Name.
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param deleteBy Field to delete records by. Permissible values are idField or dedupeFields as indicated by the
     * result of the corresponding describe record.</li>
     * <li>@param input List of input records.</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/customobjects/{customObjectName}/delete.json", method = "POST")
    Response<JsonObject> deleteCustomObjects( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query("access_token") String accessToken, //
            @Path("customObjectName") String customObjectName, //
            JsonObject payload //
    );

}
