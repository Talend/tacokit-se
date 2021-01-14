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
package org.talend.components.jdbc.input;

import org.talend.components.jdbc.configuration.InputCaptureDataChangeConfig;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.*;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;
import java.util.List;
import static java.util.Collections.singletonList;

@Version
@Icon(value = Icon.IconType.DATASTORE)
@Documentation("JDBC input using stream table name")
@PartitionMapper(name = "StreamTableNameInput", infinite = true)
public class ChangeDataCaptureInputMapper implements Serializable {

    private final InputCaptureDataChangeConfig inputConfig;

    private RecordBuilderFactory recordBuilderFactory;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    public ChangeDataCaptureInputMapper(@Option("configuration") final InputCaptureDataChangeConfig config,
            final JdbcService jdbcDriversService, final RecordBuilderFactory recordBuilderFactory,
            final I18nMessage i18nMessage) {

        this.inputConfig = config;
        this.recordBuilderFactory = recordBuilderFactory;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @Assessor
    public long estimateSize() {
        return 1000L;
    }

    @Split
    public List<ChangeDataCaptureInputMapper> split(@PartitionSize final long bundles) {
        return singletonList(this);
    }

    @Emitter
    public ChangeDataCaptureInputEmitter createWorker() {
        return new ChangeDataCaptureInputEmitter(inputConfig, jdbcDriversService, recordBuilderFactory, i18n);
    }
}
