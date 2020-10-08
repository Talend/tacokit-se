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
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.completion.DynamicValues;
import org.talend.sdk.component.api.service.completion.Values;
import org.talend.sdk.component.api.service.completion.Values.Item;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class UiAwsAuthenticationService {

    public final static String GET_AWS_REGIONS = "GET_AWS_REGIONS";

    @DynamicValues(GET_AWS_REGIONS)
    public Values getAwsRegions() {
        List<Item> amazonRegions = Arrays.stream(Regions.values()).map(r -> new Values.Item(r.getName(), r.getDescription()))
                .collect(Collectors.toList());
        return new Values(amazonRegions);
    }

}
