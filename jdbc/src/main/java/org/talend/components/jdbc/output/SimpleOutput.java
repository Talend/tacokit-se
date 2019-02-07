package org.talend.components.jdbc.output;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.talend.components.jdbc.configuration.OutputConfig;
import org.talend.components.jdbc.output.platforms.Platform;
import org.talend.components.jdbc.output.platforms.PlatformFactory;
import org.talend.components.jdbc.output.statement.QueryManagerFactory;
import org.talend.components.jdbc.output.statement.operations.QueryManagerImpl;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.processor.Processor;

import java.io.Serializable;

@Slf4j
@Getter
@Version
@Processor(name = "Output")
@Icon(value = Icon.IconType.DATASTORE)
@Documentation("JDBC Output component")
public class SimpleOutput extends Output implements Serializable {

    private QueryManagerImpl queryManager;

    private Platform platform;

    public SimpleOutput(@Option("configuration") final OutputConfig configuration, final JdbcService jdbcService,
            final I18nMessage i18n) {
        super(configuration, jdbcService, i18n);
        this.platform = PlatformFactory.get(configuration.getDataset().getConnection(), i18n);
        this.queryManager = QueryManagerFactory.getQueryManager(platform, i18n, configuration);
    }

}