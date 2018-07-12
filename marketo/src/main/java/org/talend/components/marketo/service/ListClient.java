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

public interface ListClient extends HttpClient {

    /**
     * Checks if leads are members of a given static list.
     * 
     * @param accessToken Marketo authorization token for API
     * @param listId Id of the static list to retrieve records from
     * @param leadIds Comma-separated list of lead ids to check
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}/leads/ismember.json", method = "GET")
    Response<JsonObject> isMemberOfList( //
            @Query("access_token") String accessToken, //
            @Path("listId") Integer listId, //
            @Query("id") String leadIds //
    );

    /**
     * Retrieves person records which are members of the given static list.
     * 
     * @param accessToken Marketo authorization token for API
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter.
     * @param listId Id of the static list to retrieve records from
     * @param fields Comma-separated list of field names to return changes for. Field names can be retrieved with the
     * Describe Lead API.
     * @param batchSize The batch size to return. The max and default value is 300.
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}/leads.json", method = "GET")
    Response<JsonObject> getLeadsByListId( //
            @Query("access_token") String accessToken, //
            @Query("nextPageToken") String nextPageToken, //
            @Path("listId") Integer listId, //
            @Query("fields") String fields, //
            @Query("batchSize") Integer batchSize //
    );

    /**
     * Returns a list record by its id.
     *
     * @param accessToken Marketo authorization token for API
     * @param listId Id of the static list to retrieve records from
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}.json", method = "GET")
    Response<JsonObject> getListbyId( //
            @Query("access_token") String accessToken, //
            @Path("listId") Integer listId //
    );

    /**
     * Returns a set of static list records based on given filter parameters.
     * 
     * @param accessToken Marketo authorization token for API
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter.
     * @param id Comma-separated list of static list ids to return
     * @param name Comma-separated list of static list names to return
     * @param programName Comma-separated list of program names. If set will return all static lists that are children of
     * the given programs.
     * @param workspaceName Comma-separated list of workspace names. If set will return all static lists that are children
     * of the given workspaces.
     * @param batchSize The batch size to return. The max and default value is 300.
     * @return
     */
    @Request(path = "/rest/v1/lists.json", method = "GET")
    Response<JsonObject> getLists( //
            @Query("access_token") String accessToken, //
            @Query("nextPageToken") String nextPageToken, //
            @Query("id") Integer id, //
            @Query("name") String name, //
            @Query("programName") String programName, //
            @Query("workspaceName") String workspaceName, //
            @Query("batchSize") Integer batchSize //
    );

    /**
     * Adds a given set of person records to a target static list. There is a limit of 300 lead ids per request.
     *
     * @param accessToken Marketo authorization token for API.
     * @param listId Id of the static list to add records from.
     * @param payload contains leadIds Comma-separated list of lead ids to add to the list.
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}/leads.json", method = "POST")
    Response<JsonObject> addToList( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query("access_token") String accessToken, //
            @Path("listId") Integer listId, //
            JsonObject payload //
    );

    /**
     * Removes a given set of person records from a target static list.
     * 
     * @param accessToken Marketo authorization token for API.
     * @param listId Id of static list to remove leads from.
     * @param payload contains leadIds Comma-separated list of lead ids to remove from the list.
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}/leads.json", method = "DELETE")
    Response<JsonObject> removeFromList( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query("access_token") String accessToken, //
            @Path("listId") Integer listId, //
            JsonObject payload //
    );

}
