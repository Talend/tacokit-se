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

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

/**
 * Client for managing Companies
 *
 * Companies represent the organization to which lead records belong. Leads are added to a Company by populating their
 * corresponding externalCompanyId field. Leads linked to a company record will directly inherit the values from a
 * company record as though the values existed on the lead’s own record.
 *
 * <b>Company APIs are only exposed for subscriptions which do not have a native CRM sync enabled.</b>
 */
public interface CompanyClient extends HttpClient {

    /**
     * Returns metadata about companies and the fields available for interaction via the API.
     * 
     * @param accessToken Marketo authorization token for API
     * @return metadata about companies
     */
    @Request(path = "/rest/v1/companies/describe.json")
    Response<JsonObject> describeCompanies(//
            @Query(ATTR_ACCESS_TOKEN) String accessToken //
    );

    /**
     * Retrieves company records from the destination instance based on the submitted filter.
     * 
     * @param accessToken Marketo authorization token for API
     * @param filterType The company field to filter on. Searchable fields can be retrieved with the Describe Company call
     * @param filterValues Comma-separated list of values to match against query
     * @param fields Comma-separated list of fields to include in the response query
     * @param batchSize The batch size to return. The max and default value is 300.
     * @param nextPageToken A token will be returned by this endpoint if the result set is greater than the batch size and
     * can be passed in a subsequent call through this parameter
     * @return company records
     */
    @Request(path = "/rest/v1/companies.json")
    Response<JsonObject> getCompanies(//
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_FILTER_TYPE) String filterType, //
            @Query(ATTR_FILTER_VALUES) String filterValues, //
            @Query(ATTR_FIELDS) String fields, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize, //
            @Query(ATTR_NEXT_PAGE_TOKEN) String nextPageToken //
    );

    /**
     * Allows inserting, updating, or upserting of company records into Marketo.
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
    @Request(path = "/rest/v1/companies.json", method = METHOD_POST)
    Response<JsonObject> syncCompanies(//
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );

    /**
     * Deletes the included list of company records from the destination instance.
     * 
     * @param accessToken Marketo authorization token for API
     * @param payload is json object containing the following parameters
     * <ul>
     * <li>@param deleteBy Field to delete company records by. Key may be "dedupeFields" or "idField"</li>
     * <li>@param input List of company records. Companies in the list should only contain a member matching the dedupeBy
     * value</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/companies/delete.json", method = METHOD_POST)
    Response<JsonObject> deleteCompanies(//
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            JsonObject payload //
    );
}
