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
package org.talend.components.couchbase.service;

import java.util.Arrays;

import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.Suggestions;

public class UIActionService extends CouchbaseService {

    public static final String LOAD_AVAILABLE_TIMEOUTS = "loadAvailableTimeouts";

    @Suggestions(LOAD_AVAILABLE_TIMEOUTS)
    public SuggestionValues suggestTimeoutValues() {
        return new SuggestionValues(true, Arrays.asList( //
                new SuggestionValues.Item("keyValueTimeout", "keyValueTimeout"), //
                new SuggestionValues.Item("viewTimeout", "viewTimeout"), //
                new SuggestionValues.Item("queryTimeout", "queryTimeout"), //
                new SuggestionValues.Item("searchTimeout", "searchTimeout"), //
                new SuggestionValues.Item("analyticsTimeout", "analyticsTimeout"), //
                new SuggestionValues.Item("connectionTimeout", "connectionTimeout"), //
                new SuggestionValues.Item("disconnectTimeout", "disconnectTimeout"), //
                new SuggestionValues.Item("managementTimeout", "managementTimeout"), //
                new SuggestionValues.Item("socketConnectTimeout", "socketConnectTimeout") //
        ));
    }

}
