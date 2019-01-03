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
package org.talend.components.jdbc.input;

import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.InputQueryConfig;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.service.record.RecordBuilderFactory;

import java.io.Serializable;

@Slf4j
@Version
@Icon(value = Icon.IconType.DATASTORE)
@Emitter(name = "QueryInput")
@Documentation("JDBC query input")
public class QueryInputEmitter extends AbstractInputEmitter implements Serializable {

    public QueryInputEmitter(@Option("configuration") final InputQueryConfig inputQueryConfig,
            final JdbcService jdbcDriversService, final RecordBuilderFactory recordBuilderFactory,
            final I18nMessage i18nMessage) {
        super(inputQueryConfig, jdbcDriversService, recordBuilderFactory, i18nMessage);
    }

}
