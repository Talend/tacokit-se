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
package org.talend.components.adlsgen2.input;

import java.io.Serializable;
import java.util.Iterator;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.adlsgen2.service.AdlsGen2Service;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.service.Service;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Version(1)
@Documentation("Azure Data Lake Storage Gen2 Input")
public class AdlsGen2Input implements Serializable {

    @Service
    private final AdlsGen2Service service;

    private InputConfiguration configuration;

    private Iterator<Record> records;

    public AdlsGen2Input(@Option("configuration") final InputConfiguration configuration, final AdlsGen2Service service) {
        this.configuration = configuration;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        records = service.pathRead(configuration);
    }

    @Producer
    public Record next() {
        return records.next();
    }

    @PreDestroy
    public void release() {
    }
}
