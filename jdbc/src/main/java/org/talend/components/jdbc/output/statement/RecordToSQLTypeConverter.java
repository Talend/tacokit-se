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
package org.talend.components.jdbc.output.statement;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.talend.sdk.component.api.record.Record;

public enum RecordToSQLTypeConverter {
    RECORD {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setObject(index, record.get(Record.class, column).toString());
        }
    },
    ARRAY {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setArray(index,
                    statement.getConnection().createArrayOf(column, record.getArray(Object.class, column).toArray()));
        }
    },
    STRING {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setString(index, record.getString(column));
        }
    },
    BYTES {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setBytes(index, record.getBytes(column));
        }
    },
    INT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setInt(index, record.getInt(column));
        }
    },
    LONG {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setLong(index, record.getLong(column));
        }
    },
    FLOAT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setFloat(index, record.getFloat(column));
        }
    },
    DOUBLE {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setDouble(index, record.getDouble(column));
        }
    },
    BOOLEAN {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setBoolean(index, record.getBoolean(column));
        }
    },
    DATETIME {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
                throws SQLException {
            statement.setDate(index, Date.valueOf(record.getDateTime(column).toLocalDate()));
        }
    };

    public abstract void setValue(final PreparedStatement statement, final int index, final String column, final Record record)
            throws SQLException;
}
