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
