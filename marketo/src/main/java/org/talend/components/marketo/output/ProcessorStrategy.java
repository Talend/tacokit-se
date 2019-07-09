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
package org.talend.components.marketo.output;

import java.util.List;

import javax.json.JsonObject;

import static java.util.Arrays.asList;
import static org.talend.components.marketo.MarketoApiConstants.ATTR_STATUS;

public interface ProcessorStrategy {

    void init();

    /**
     * Generate payload for API call
     *
     * @param incomingData list of elements to process
     * @return
     */
    JsonObject getPayload(List<JsonObject> incomingData);

    /**
     * Execute the processor's action according configuration parameters
     *
     * @param payload
     * @return
     */
    JsonObject runAction(JsonObject payload);

    /**
     * Create a reject flow record based on response data. Default implementation returns the object as it is.
     *
     * @param data
     * @return
     */
    // default JsonObject createRejectData(JsonObject data) {
    // return data;
    // }

    /**
     * Create a main flow record based on response data. Default implementation returns the object as it is.
     *
     * @param data
     * @return
     */
    // default JsonObject createMainData(JsonObject data) {
    // return data;
    // }

    /**
     * Check if API response for the operation is not successful.
     *
     * @param result the status returned by API call.
     * @return
     */
    default boolean isRejected(JsonObject result) {
        return !asList("created", "updated", "deleted", "scheduled", "triggered", "added", "removed", "memberof", "notmemberof")
                .contains(result.getString(ATTR_STATUS).toLowerCase());
    }

}
