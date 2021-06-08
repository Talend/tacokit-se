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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.talend.components.couchbase.configuration.ConnectionConfiguration;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.SuggestionValues;
import org.talend.sdk.component.api.service.completion.SuggestionValues.Item;
import org.talend.sdk.component.api.service.completion.Suggestions;

@Service
public class UIActionService {

    public static final String LOAD_PARAMETERS = "loadParameters";

    @Suggestions(LOAD_PARAMETERS)
    public SuggestionValues loadParameters(ConnectionConfiguration conf) {
        List<Item> list = new ArrayList<>();
        list.add(new SuggestionValues.Item("01", "keyValueTimeout"));
        list.add(new SuggestionValues.Item("02", "viewTimeout"));
        return new SuggestionValues(true, list);
        /*
         * return new SuggestionValues(true, Arrays.asList( //
         * new SuggestionValues.Item("01", "keyValueTimeout"), //
         * new SuggestionValues.Item("02", "viewTimeout"), //
         * new SuggestionValues.Item("03", "queryTimeout"), //
         * new SuggestionValues.Item("04", "searchTimeout"), //
         * new SuggestionValues.Item("05", "analyticsTimeout"), //
         * new SuggestionValues.Item("06", "connectionTimeout"), //
         * new SuggestionValues.Item("07", "disconnectTimeout"), //
         * new SuggestionValues.Item("08", "managementTimeout"), //
         * new SuggestionValues.Item("09", "socketConnectTimeout") //
         * ));
         */
    }

}
