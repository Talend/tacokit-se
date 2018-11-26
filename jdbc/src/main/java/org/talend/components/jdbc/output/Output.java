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

import com.zaxxer.hikari.HikariDataSource;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfiguration;
import org.talend.components.jdbc.output.statement.JdbcActionFactory;
import org.talend.components.jdbc.output.statement.operations.JdbcAction;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.*;
import org.talend.sdk.component.api.record.Record;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.Serializable;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Processor(name = "Output")
@Version
@Icon(value = Icon.IconType.CUSTOM, custom = "JDBCOutput")
@Documentation("JDBC Output component")
public class Output implements Serializable {

    private final OutputConfiguration configuration;

    private final JdbcService jdbcService;

    private final I18nMessage i18n;

    private transient List<Record> records;

    private transient JdbcAction jdbcAction;

    private transient HikariDataSource dataSource;

    public Output(@Option("configuration") final OutputConfiguration outputConfiguration, final JdbcService jdbcService,
            final I18nMessage i18nMessage) {
        this.configuration = outputConfiguration;
        this.jdbcService = jdbcService;
        this.i18n = i18nMessage;
    }

    @PostConstruct
    public void init() {
        dataSource = jdbcService.createDataSource(configuration.getDataset().getConnection(),
                configuration.isRewriteBatchedStatements());
        this.jdbcAction = new JdbcActionFactory(i18n, dataSource, configuration).createAction();
        this.records = new ArrayList<>();
    }

    @BeforeGroup
    public void beforeGroup() {
        records.clear();
    }

    @ElementListener
    public void elementListener(@Input final Record record) {
        records.add(record);
    }

    @AfterGroup
    public void afterGroup() throws SQLException {
        // TODO : handle discarded records
        try {
            final List<Reject> discards = jdbcAction.execute(records);
            discards.stream().map(Object::toString).forEach(log::info);
        } catch (final Exception e) {
            records.stream().map(Object::toString).forEach(log::info);
            throw e;
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

}
