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
package org.talend.components.jdbc.output.platforms;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import org.talend.components.jdbc.configuration.DistributionStrategy;
import org.talend.components.jdbc.configuration.RedshiftSortStrategy;
import org.talend.components.jdbc.service.I18nMessage;

import lombok.extern.slf4j.Slf4j;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

/**
 * https://docs.aws.amazon.com/fr_fr/redshift/latest/dg/r_CREATE_TABLE_NEW.html
 */
@Slf4j
public class RedshiftPlatform extends Platform {

    public static final String REDSHIFT = "redshift";

    public RedshiftPlatform(final I18nMessage i18n) {
        super(i18n);
    }

    @Override
    public String name() {
        return REDSHIFT;
    }

    @Override
    protected String delimiterToken() {
        // https://docs.aws.amazon.com/redshift/latest/dg/r_names.html
        return "\"";
    }

    @Override
    protected String buildQuery(final Connection connection, final Table table) throws SQLException {
        // keep the string builder for readability
        final StringBuilder sql = new StringBuilder("CREATE TABLE");
        sql.append(" ");
        sql.append("IF NOT EXISTS");
        sql.append(" ");
        if (table.getSchema() != null && !table.getSchema().isEmpty()) {
            sql.append(identifier(table.getSchema())).append(".");
        }
        sql.append(identifier(table.getName()));
        sql.append("(");
        List<Column> columns = table.getColumns();
        sql.append(createColumns(columns, table.getSortStrategy(), columns.stream().filter(Column::isSortKey).collect(toList())));
        sql.append(createPKs(connection.getMetaData(), table.getName(),
                columns.stream().filter(Column::isPrimaryKey).collect(toList())));
        sql.append(")");
        sql.append(createDistributionKeys(table.getDistributionStrategy(),
                columns.stream().filter(Column::isDistributionKey).collect(toList())));
        if (RedshiftSortStrategy.COMPOUND.equals(table.getSortStrategy())
                || RedshiftSortStrategy.INTERLEAVED.equals(table.getSortStrategy())) {
            sql.append(createSortKeys(table.getSortStrategy(), columns.stream().filter(Column::isSortKey).collect(toList())));
        }

        log.info("Database - create table query ");
        log.info(sql.toString());
        return sql.toString();
    }

    private String createSortKeys(final RedshiftSortStrategy sortStrategy, final List<Column> columns) {
        return columns.isEmpty() ? ""
                : sortStrategy.name() + " sortkey" + columns.stream().map(Column::getName).collect(joining(",", "(", ")"));
    }

    private String createDistributionKeys(final DistributionStrategy distributionStrategy, final List<Column> columns) {
        switch (distributionStrategy) {
        case ALL:
            return " diststyle all ";
        case EVEN:
            return " diststyle even ";
        case KEYS:
            return columns.isEmpty() ? ""
                    : " diststyle key distkey" + columns.stream().map(Column::getName).collect(joining(",", "(", ") "));
        default:
        case AUTO:
            return " diststyle auto ";
        }
    }

    @Override
    protected boolean isTableExistsCreationError(final Throwable e) {
        // name space creation issue in distributed exectution is not handled by "IF NOT EXISTS"
        // https://www.postgresql.org/message-id/CA%2BTgmoZAdYVtwBfp1FL2sMZbiHCWT4UPrzRLNnX1Nb30Ku3-gg%40mail.gmail.com
        return e instanceof SQLException && "23505".equals(((SQLException) e).getSQLState());
    }

    private String createColumns(final List<Column> columns, final RedshiftSortStrategy sortStrategy,
            final List<Column> sortKeys) {
        return columns.stream().map(c -> createColumn(c, sortStrategy, sortKeys)).collect(Collectors.joining(","));
    }

    private String createColumn(final Column column, final RedshiftSortStrategy sortStrategy, final List<Column> sortKeys) {
        return identifier(column.getName())//
                + " " + toDBType(column)//
                + " " + isRequired(column)//
                + (RedshiftSortStrategy.SINGLE.equals(sortStrategy) && sortKeys.contains(column) ? " sortkey" : "");
    }

    private String toDBType(final Column column) {
        switch (column.getType()) {
        case STRING:
            // https://docs.aws.amazon.com/fr_fr/redshift/latest/dg/r_Character_types.html
            return column.getSize() <= -1 ? "VARCHAR(max)" : "VARCHAR(" + column.getSize() + ")";
        case BOOLEAN:
            return "BOOLEAN";
        case DOUBLE:
            return "REAL";
        case FLOAT:
            return "DOUBLE PRECISION";
        case LONG:
            return "BIGINT";
        case INT:
            return "INTEGER";
        case DATETIME:
            return "TIMESTAMP";
        case BYTES:
            throw new IllegalStateException(getI18n().errorRedshiftUnsupportedBytes(column.getName()));
        case RECORD:
        case ARRAY:
        default:
            throw new IllegalStateException(getI18n().errorUnsupportedType(column.getType().name(), column.getName()));
        }
    }

}
