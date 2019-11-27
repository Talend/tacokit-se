/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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
package org.talend.components.magentocms;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.talend.components.magentocms.common.AuthenticationLoginPasswordConfiguration;
import org.talend.components.magentocms.common.AuthenticationOauth1Configuration;
import org.talend.components.magentocms.common.AuthenticationTokenConfiguration;
import org.talend.components.magentocms.common.AuthenticationType;
import org.talend.components.magentocms.common.MagentoDataStore;
import org.talend.components.magentocms.common.UnknownAuthenticationTypeException;
import org.talend.components.magentocms.input.SelectionFilter;
import org.talend.components.magentocms.input.SelectionFilterOperator;
import org.talend.components.magentocms.service.MagentoCmsService;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Slf4j
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class MagentoTest {

    @Test
    void testConnection() throws UnknownAuthenticationTypeException {
        log.info("Test 'Connection' start ");
        AuthenticationOauth1Configuration authenticationOauth1Settings = new AuthenticationOauth1Configuration();
        AuthenticationTokenConfiguration authenticationTokenSettings = new AuthenticationTokenConfiguration();
        AuthenticationLoginPasswordConfiguration authenticationLoginPasswordSettings = new AuthenticationLoginPasswordConfiguration();
        MagentoDataStore magentoDataStore;
        magentoDataStore = new MagentoDataStore(null, null, AuthenticationType.OAUTH_1, authenticationOauth1Settings,
                authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationOauth1Settings, magentoDataStore.getAuthSettings());
        magentoDataStore = new MagentoDataStore(null, null, AuthenticationType.AUTHENTICATION_TOKEN, authenticationOauth1Settings,
                authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationTokenSettings, magentoDataStore.getAuthSettings());
        magentoDataStore = new MagentoDataStore(null, null, AuthenticationType.LOGIN_PASSWORD, authenticationOauth1Settings,
                authenticationTokenSettings, authenticationLoginPasswordSettings);
        assertEquals(authenticationLoginPasswordSettings, magentoDataStore.getAuthSettings());
    }

    @ParameterizedTest
    @MethodSource("methodSourceAdvancedFilterSuggestion")
    void testAdvancedFilterSuggestion(SelectionFilterOperator selectionFilterOperator, String rightResult) {
        log.info("Test 'Advanced filter suggestion' start. SelectionFilterOperator: " + selectionFilterOperator);
        List<SelectionFilter> filterLines = new ArrayList<>();
        filterLines.add(new SelectionFilter("sku", "eq", "24-MB01"));
        filterLines.add(new SelectionFilter("sku", "like", "M%"));
        String suggestion = new MagentoCmsService().updatableFilterAdvanced(selectionFilterOperator, filterLines)
                .getFilterAdvancedValue();
        assertEquals(rightResult, suggestion);
    }

    private Stream<Arguments> methodSourceAdvancedFilterSuggestion() {
        return Stream.of(
                Arguments.of(SelectionFilterOperator.AND,
                        "searchCriteria[filter_groups][0][filters][0][condition_type]=eq"
                                + "&searchCriteria[filter_groups][0][filters][0][field]=sku"
                                + "&searchCriteria[filter_groups][0][filters][0][value]=24-MB01"
                                + "&searchCriteria[filter_groups][1][filters][0][condition_type]=like"
                                + "&searchCriteria[filter_groups][1][filters][0][field]=sku"
                                + "&searchCriteria[filter_groups][1][filters][0][value]=M%"),
                Arguments.of(SelectionFilterOperator.OR,
                        "searchCriteria[filter_groups][0][filters][0][condition_type]=eq"
                                + "&searchCriteria[filter_groups][0][filters][0][field]=sku"
                                + "&searchCriteria[filter_groups][0][filters][0][value]=24-MB01"
                                + "&searchCriteria[filter_groups][0][filters][1][condition_type]=like"
                                + "&searchCriteria[filter_groups][0][filters][1][field]=sku"
                                + "&searchCriteria[filter_groups][0][filters][1][value]=M%"));
    }
}
