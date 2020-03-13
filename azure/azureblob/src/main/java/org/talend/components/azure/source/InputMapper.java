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
package org.talend.components.azure.source;

import static java.util.Collections.singletonList;

import java.io.Serializable;
import java.util.List;

import org.talend.components.azure.service.MessageService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Assessor;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.PartitionMapper;
import org.talend.sdk.component.api.input.PartitionSize;
import org.talend.sdk.component.api.input.Split;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import org.talend.components.azure.service.AzureBlobComponentServices;

//
// this class role is to enable the work to be distributed in environments supporting it.
//
@Version(1) // default version is 1, if some configuration changes happen between 2 versions you can add a migrationHandler
@Icon(value = Icon.IconType.CUSTOM, custom = "talend-azure")
@PartitionMapper(name = "Input")
@Documentation("Mapper for AzureBlobStorage Readers")
public class InputMapper implements Serializable {

    private final BlobInputProperties configuration;

    private final AzureBlobComponentServices service;

    private final RecordBuilderFactory recordBuilderFactory;

    private final MessageService messageService;

    public InputMapper(@Option("configuration") final BlobInputProperties configuration, final AzureBlobComponentServices service,
            final RecordBuilderFactory recordBuilderFactory, final MessageService messageService) {
        this.configuration = configuration;
        this.service = service;
        this.recordBuilderFactory = recordBuilderFactory;
        this.messageService = messageService;
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
    public BlobSource createWorker() {
        return new BlobSource(configuration, service, recordBuilderFactory, messageService);
    }
}