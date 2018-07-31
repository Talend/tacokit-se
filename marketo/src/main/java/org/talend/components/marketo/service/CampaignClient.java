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
import static org.talend.components.marketo.MarketoApiConstants.ATTR_CAMPAIGN_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_ID;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_IS_TRIGGERABLE;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_PROGRAM_NAME;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_WORKSPACE_NAME;
import static org.talend.components.marketo.MarketoApiConstants.HEADER_CONTENT_TYPE;
import static org.talend.components.marketo.MarketoApiConstants.METHOD_POST;

import javax.json.JsonObject;

import org.talend.sdk.component.api.service.http.Header;
import org.talend.sdk.component.api.service.http.HttpClient;
import org.talend.sdk.component.api.service.http.Path;
import org.talend.sdk.component.api.service.http.Query;
import org.talend.sdk.component.api.service.http.Request;
import org.talend.sdk.component.api.service.http.Response;

public interface CampaignClient extends HttpClient {

    /**
     * Returns a list of campaign records.
     *
     * @param accessToken Marketo authorization token for API
     * @param id Comma-separated list of campaign ids to return records for.
     * @param name Comma-separated list of names to filter on.
     * @param programName Comma-separated list of program names to filter on. If set, will filter to only campaigns which
     * are children of the designated programs.
     * @param workspaceName Comma-separated list of workspace names to filter on. If set, will only return campaigns in the
     * given workspaces.
     * @param batchSize Maximum number of records to return. Maximum and default is 300.
     * @param isTriggerable Set to true to return active Campaigns which have a Campaign is Requested trigger and source is
     * Web Service API
     * @return
     */
    @Request(path = "/rest/v1/campaigns.json")
    Response<JsonObject> getCampaigns( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Query(ATTR_ID) String id, //
            @Query(ATTR_NAME) String name, //
            @Query(ATTR_PROGRAM_NAME) String programName, //
            @Query(ATTR_WORKSPACE_NAME) String workspaceName, //
            @Query(ATTR_BATCH_SIZE) Integer batchSize, //
            @Query(ATTR_IS_TRIGGERABLE) Boolean isTriggerable //
    );

    /**
     * Returns the record of a campaign by its id.
     * 
     * @param accessToken Marketo authorization token for API
     * @param campaignId the Id of the campaign
     * @return
     */
    @Request(path = "/rest/v1/campaigns/{campaignId}.json")
    Response<JsonObject> getCampaignById( //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_CAMPAIGN_ID) Integer campaignId //
    );

    /**
     * Remotely schedules a batch campaign to run at a given time. My tokens local to the campaign's parent program can be
     * overridden for the run to customize content. When using the "cloneToProgramName" parameter described below, this
     * endpoint is limited to 20 calls per day.
     * 
     * @param accessToken Marketo authorization token for API
     * @param campaignId Id of the batch campaign to schedule.
     * @param input JsonObject containing the following properties:
     * <ul>
     * <li>cloneToProgramName (string, optional): Name of the resulting program. When set, this attribute will cause the
     * campaign, parent program, and all of its assets, to be created with the resulting new name. The parent program will
     * be cloned and the newly created campaign will be scheduled. The resulting program is created underneath the parent.
     * Programs with snippets, push notifications, in-app messages, static lists, reports, and social assets may not be
     * cloned in this way</li>
     * <li>runAt (string, optional): Datetime to run the campaign at. If unset, the campaign will be run five minutes after
     * the call is made</li>
     * <li>tokens (Array[Token], optional): List of my tokens to replace during the run of the target campaign. The tokens
     * must be available in a parent program or folder to be replaced during the run</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/campaigns/{campaignId}/schedule.json", method = METHOD_POST)
    Response<JsonObject> scheduleCampaign( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_CAMPAIGN_ID) Integer campaignId, //
            JsonObject input//
    );

    /**
     * Activates a trigger smart campaign.
     * 
     * @param accessToken Marketo authorization token for API
     * @param id Id of the smart campaign
     * @return
     */
    @Request(path = "/rest/asset/v1/smartCampaign/{id}/activate.json", method = METHOD_POST)
    Response<JsonObject> activateSmartCampaign( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_ID) Integer id //
    );

    /**
     * Deactivates a trigger smart campaign.
     * 
     * @param accessToken Marketo authorization token for API
     * @param id Id of the smart campaign
     * @return
     */
    @Request(path = "/rest/asset/v1/smartCampaign/{id}/deactivate.json", method = METHOD_POST)
    Response<JsonObject> deactivateSmartCampaign( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_ID) Integer id //
    );

    /**
     * Passes a set of leads to a trigger campaign to run through the campaign's flow. The designated campaign must have a
     * Campaign is Requested: Web Service API trigger, and must be active. My tokens local to the campaign's parent program
     * can be overridden for the run to customize content. A maximum of 100 leads are allowed per call.
     * 
     * @param accessToken Marketo authorization token for API
     * @param campaignId Id of the batch campaign to schedule.
     * @param input JsonObject containing the following properties:
     * <ul>
     * <li>leads (Array[InputLead]): List of leads for input</li>
     * <li>tokens (Array[Token], optional): List of my tokens to replace during the run of the target campaign. The tokens
     * must be available in a parent program or folder to be replaced during the run</li>
     * </ul>
     * @return
     */
    @Request(path = "/rest/v1/campaigns/{campaignId}/trigger.json", method = METHOD_POST)
    Response<JsonObject> requestCampaign( //
            @Header(HEADER_CONTENT_TYPE) String contentType, //
            @Query(ATTR_ACCESS_TOKEN) String accessToken, //
            @Path(ATTR_CAMPAIGN_ID) Integer campaignId, //
            JsonObject input//
    );

}
