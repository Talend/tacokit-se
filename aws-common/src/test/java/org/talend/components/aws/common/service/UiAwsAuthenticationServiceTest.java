/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.aws.common.service;

import com.amazonaws.regions.Regions;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.talend.components.aws.common.service.UiAwsAuthenticationService;
import org.talend.sdk.component.api.service.completion.Values.Item;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UiAwsAuthenticationServiceTest {

    @Test
    public void testRegionsIds() {
        UiAwsAuthenticationService service = new UiAwsAuthenticationService();
        List<String> regionIds = service.getAwsRegions().getItems().stream().map(Item::getId).collect(Collectors.toList());
        List<String> regionIdsExpected = Arrays.stream(Regions.values()).map(Regions::getName).collect(Collectors.toList());
        Assertions.assertEquals(regionIdsExpected.size(), regionIds.size());
        MatcherAssert.assertThat(regionIds, Matchers.containsInAnyOrder(regionIdsExpected.toArray()));
    }

    @Test
    public void testRegionsNames() {
        UiAwsAuthenticationService service = new UiAwsAuthenticationService();
        List<String> regionNames = service.getAwsRegions().getItems().stream().map(Item::getLabel).collect(Collectors.toList());
        List<String> regionNamesExpected = Arrays.stream(Regions.values()).map(Regions::getDescription)
                .collect(Collectors.toList());
        Assertions.assertEquals(regionNamesExpected.size(), regionNames.size());
        MatcherAssert.assertThat(regionNames, Matchers.containsInAnyOrder(regionNamesExpected.toArray()));
    }

}
