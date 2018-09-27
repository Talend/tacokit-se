package org.talend.components.jdbc.output;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.talend.components.jdbc.JdbcConfiguration;
import org.talend.components.jdbc.output.internal.StatementManager;
import org.talend.components.jdbc.service.I18nMessage;
import org.talend.components.jdbc.service.JdbcService;
import org.talend.components.jdbc.dataset.OutputDataset;
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
import org.talend.sdk.component.api.service.configuration.Configuration;

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

    private Connection connection;

    private PreparedStatement preparedStatement = null;

    private StatementManager statementManager;

    private JdbcConfiguration jdbcConfiguration;

    public Output(@Option("configuration") final OutputDataset dataset,
            @Configuration("jdbc") JdbcConfiguration jdbcConfiguration, final JdbcService jdbcDriversService,
            final I18nMessage i18nMessage) {

        this.dataset = dataset;
        this.jdbcDriversService = jdbcDriversService;
        this.i18n = i18nMessage;
        this.jdbcConfiguration = jdbcConfiguration;
    }

    @PostConstruct
    public void init() {
        connection = jdbcDriversService.connection(dataset.getConnection(), jdbcConfiguration);
        try {
            connection.setAutoCommit(false);
        } catch (SQLException e) {
            log.error("Can't deactivate auto-commit, this may alter the performance if this batch");
        }

        this.statementManager = StatementManager.get(dataset);
    }

    @BeforeGroup
    public void beforeGroup() {
        if (preparedStatement != null) {
            try {
                preparedStatement.clearParameters();
                preparedStatement.clearBatch();
            } catch (SQLException e) {
                throw new IllegalStateException("Can't clear prepared statement.");
            }
        }
    }

    @ElementListener
    public void elementListener(@Input final Record record) {
        try {
            if (preparedStatement == null) {
                switch (dataset.getActionOnData()) {
                default:
                case Insert:
                    preparedStatement = connection.prepareStatement(statementManager.createQuery(record));
                    break;
                case Delete:
                    preparedStatement = connection.prepareStatement(statementManager.createQuery(record));
                    break;
                case Update:
                    preparedStatement = connection.prepareStatement(statementManager.createQuery(record));
                    break;
                }
            }

            try {
                statementManager.populateParameters(preparedStatement, record);
                preparedStatement.addBatch();
            } catch (SQLException e) {
                // todo : how to handle record errors ? => ignored for now
                log.error("error with record " + record.toString() + "\nThe record was ignored.", e);
            }

        } catch (final SQLException e) {
            throw new IllegalStateException("can't create the prepared statement", e);
        }

    }

    @AfterGroup
    public void afterGroup() {
        try {
            final int[] result = preparedStatement.executeBatch();
            if (!connection.getAutoCommit()) {
                connection.commit();
            }
        } catch (SQLException e) {
            // rollback the group
            try {
                connection.rollback();
            } catch (SQLException e1) {
                log.error("Can't rollback statements", e);
            }

            // fixme : should we transform this component to a processor and :
            // 2 . emit rejected records

            StringBuilder batchErrorMessage = new StringBuilder();
            SQLException batchError = e;
            while (batchError.getNextException() != null) {
                batchErrorMessage.append("- ").append(batchError.getNextException().getLocalizedMessage()).append("\n");
                batchError = batchError.getNextException();
            }
            throw new IllegalStateException(batchErrorMessage.toString(), e);
        }
    }

    @PreDestroy
    public void preDestroy() {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException e) {
                log.warn("Can't close the prepared statement properly.", e);
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                log.warn("Can't close the connection properly.", e);
            } finally {
                connection = null;
            }
        }
    }

}
