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
package org.talend.components.jdbc.output.statement;

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.*;

import static java.util.Optional.ofNullable;

public enum RecordToSQLTypeConverter {
    RECORD {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setObject(index, record.get(Record.class, entry.getName()).toString());
        }

        @Override
        public int getSQLType() {
            return Types.JAVA_OBJECT;
        }
    },
    ARRAY {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setArray(index, statement.getConnection().createArrayOf(entry.getName(),
                    record.getArray(Object.class, entry.getName()).toArray()));
        }

        @Override
        public int getSQLType() {
            return Types.ARRAY;
        }
    },
    STRING {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setString(index, record.getString(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.VARCHAR;
        }
    },
    BYTES {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setBytes(index, record.getBytes(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.BLOB;
        }
    },
    INT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setInt(index, record.getInt(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.INTEGER;
        }
    },
    LONG {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setLong(index, record.getLong(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.BIGINT;
        }
    },
    FLOAT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setFloat(index, record.getFloat(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.FLOAT;
        }
    },
    DOUBLE {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setDouble(index, record.getDouble(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.DOUBLE;
        }
    },
    BOOLEAN {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setBoolean(index, record.getBoolean(entry.getName()));
        }

        @Override
        public int getSQLType() {
            return Types.BOOLEAN;
        }
    },
    DATETIME {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setTimestamp(index, ofNullable(record.getDateTime(entry.getName()))
                    .map(d -> new Timestamp(d.toInstant().toEpochMilli())).orElse(null));
        }

        @Override
        public int getSQLType() {
            return Types.DATE;
        }
    };

    public abstract void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
            final Record record) throws SQLException;

    public abstract int getSQLType();
}
