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
package org.talend.components.assertion.service;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
//import org.talend.sdk.component.api.service.Service;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import javax.json.JsonBuilderFactory;
import javax.json.JsonReaderFactory;
import javax.json.spi.JsonProvider;

@Slf4j
@org.talend.sdk.component.api.service.Service
@Data
public class Service {

    @org.talend.sdk.component.api.service.Service
    private JsonReaderFactory jsonReaderFactory;

    @org.talend.sdk.component.api.service.Service
    private RecordBuilderFactory recordBuilderFactory;

    @org.talend.sdk.component.api.service.Service
    private JsonBuilderFactory jsonBuilderFactory;

    @org.talend.sdk.component.api.service.Service
    private JsonProvider jsonProvider;


}
