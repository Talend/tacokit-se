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
package org.talend.components.magentocms.input;

import org.talend.components.magentocms.helpers.ConfigurationHelper;
import org.talend.components.magentocms.service.MagentoCmsService;
import org.talend.components.magentocms.service.http.MagentoHttpClientService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;

import java.io.Serializable;
import java.util.List;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "magento_input")
@PartitionMapper(name = "Input")
@Documentation("Input mapper class")
public class MagentoCmsInputMapper implements Serializable {

    private final MagentoInputConfiguration configuration;

    private final MagentoHttpClientService magentoHttpClientService;

    private final MagentoCmsService magentoCmsService;

    public MagentoCmsInputMapper(@Option("configuration") final MagentoInputConfiguration configuration,
            MagentoHttpClientService magentoHttpClientService, MagentoCmsService magentoCmsService) {
        this.configuration = configuration;
        this.magentoHttpClientService = magentoHttpClientService;
        this.magentoCmsService = magentoCmsService;
        ConfigurationHelper.setupServicesInput(configuration, magentoHttpClientService);
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<MagentoCmsInputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter(name = "Input")
    public MagentoInput createWorker() {
        return new MagentoInput(configuration, magentoHttpClientService, magentoCmsService);
    }
}