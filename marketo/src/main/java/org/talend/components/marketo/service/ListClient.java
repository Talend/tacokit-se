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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_LIST_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NEXT_PAGE_TOKEN;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_PROGRAM_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_WORKSPACE_NAME;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.METHOD_DELETE;
import static org.talend.components.marketo.MarketoApiConstants.METHOD_POST;

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
    @Request(path = "/rest/v1/lists/{listId}/leads/ismember.json")
    Response<JsonObject> isMemberOfList( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_LIST_ID) Integer listId, //
            @Query(ATTR_ID) String leadIds //
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
    @Request(path = "/rest/v1/lists/{listId}/leads.json")
    Response<JsonObject> getLeadsByListId( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken, //
            @Path(ATTR_LIST_ID) Integer listId, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize //
    );

    /**
     * Returns a list record by its id.
     *
     * @param accessToken Marketo authorization token for API
     * @param listId Id of the static list to retrieve records from
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}.json")
    Response<JsonObject> getListbyId( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_LIST_ID) Integer listId //
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
    @Request(path = "/rest/v1/lists.json")
    Response<JsonObject> getLists( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken, //
            @Query(ATTR_ID) Integer id, //
            @Query(ATTR_NAME) String name, //
            @Query(ATTR_PROGRAM_NAME) String programName, //
            @Query(ATTR_WORKSPACE_NAME) String workspaceName, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize //
    );

    /**
     * Adds a given set of person records to a target static list. There is a limit of 300 lead ids per request.
     *
     * @param accessToken Marketo authorization token for API.
     * @param listId Id of the static list to add records from.
     * @param payload contains leadIds Comma-separated list of lead ids to add to the list.
     * @return
     */
    @Request(path = "/rest/v1/lists/{listId}/leads.json", method = METHOD_POST)
    Response<JsonObject> addToList( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_LIST_ID) Integer listId, //
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
    @Request(path = "/rest/v1/lists/{listId}/leads.json", method = METHOD_DELETE)
    Response<JsonObject> removeFromList( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_LIST_ID) Integer listId, //
            JsonObject payload //
    );

}
