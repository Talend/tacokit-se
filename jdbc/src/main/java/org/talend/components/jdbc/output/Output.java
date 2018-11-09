/*
 * Copyright (C) 2006-2018 Talend Inc. - www.talend.com
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
package org.talend.components.jdbc.output;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.dataset.OutputDataset;
import org.talend.components.jdbc.output.internal.StatementManager;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.AfterGroup;
import org.talend.sdk.component.api.processor.BeforeGroup;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Input;
import org.talend.sdk.component.api.processor.Processor;
import org.talend.sdk.component.api.record.Record;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputDataset dataset;

    private final JdbcService jdbcDriversService;

    private final I18nMessage i18n;

    private transient StatementManager statementManager;

    public Output(@Option("configuration") final OutputDataset dataset, final JdbcService jdbcDriversService,
            final I18nMessage i18nMessage) {
        this.dataset = dataset;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {

    }

    @BeforeGroup
    public void beforeGroup() {
        final Connection connection = jdbcDriversService.connection(dataset.getConnection());
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            log.error("Can't deactivate auto-commit, this may alter the performance if this batch");
        }

        this.statementManager = StatementManager.get(dataset, connection, i18n);

    }

    @ElementListener
    public void elementListener(@Input final Record record) {
        statementManager.addBatch(record);
    }

    @AfterGroup
    public void afterGroup() {
        statementManager.executeBatch();
        this.statementManager.clear();
        statementManager.close();
    }

    @PreDestroy
    public void preDestroy() {

    }

}
