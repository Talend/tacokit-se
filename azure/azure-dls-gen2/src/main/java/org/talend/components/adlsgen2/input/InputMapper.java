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
package org.talend.components.adlsgen2.input;

import java.io.Serializable;
import java.util.List;

import javax.json.JsonBuilderFactory;

import org.talend.components.adlsgen2.service.AdlsActiveDirectoryService;
import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import static java.util.Collections.singletonList;

@Version(1)
@Icon(value = Icon.IconType.CUSTOM, custom = "AdlsGen2Input")
@PartitionMapper(name = "AdlsGen2Input")
@Documentation("Mapper for Azure Data Lake Storage Gen2")
public class InputMapper implements Serializable {

    @Service
    private final AdlsGen2Service service;

    @Service
    private final RecordBuilderFactory recordBuilderFactory;

    @Service
    private final AdlsActiveDirectoryService tokenProviderService;

    private final InputConfiguration configuration;

    private final JsonBuilderFactory jsonBuilderFactory;

    public InputMapper(@Option("configuration") final InputConfiguration configuration, final AdlsGen2Service service,
            final RecordBuilderFactory recordBuilderFactory, final JsonBuilderFactory jsonBuilderFactory,
            AdlsActiveDirectoryService tokenProviderService) {
        this.configuration = configuration;
        this.service = service;
        this.tokenProviderService = tokenProviderService;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jsonBuilderFactory = jsonBuilderFactory;
    }

    @Assessor
    public long estimateSize() {
        return 1L;
    }

    @Split
    public List<InputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter
    public AdlsGen2Input createWorker() {
        return new AdlsGen2Input(configuration, service, recordBuilderFactory, jsonBuilderFactory, tokenProviderService);
    }
}
