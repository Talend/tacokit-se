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

import org.talend.sdk.component.api.record.Record;
import org.talend.sdk.component.api.record.Schema;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import static java.util.Optional.ofNullable;

public enum RecordToSQLTypeConverter {
    RECORD {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setObject(index, record.get(Record.class, entry.getName()).toString());
        }
    },
    ARRAY {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setArray(index, statement.getConnection().createArrayOf(entry.getName(),
                    record.getArray(Object.class, entry.getName()).toArray()));
        }
    },
    STRING {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setString(index, record.getString(entry.getName()));
        }
    },
    BYTES {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setBytes(index, record.getBytes(entry.getName()));
        }
    },
    INT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setInt(index, record.getInt(entry.getName()));
        }
    },
    LONG {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setLong(index, record.getLong(entry.getName()));
        }
    },
    FLOAT {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setFloat(index, record.getFloat(entry.getName()));
        }
    },
    DOUBLE {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setDouble(index, record.getDouble(entry.getName()));
        }
    },
    BOOLEAN {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setBoolean(index, record.getBoolean(entry.getName()));
        }
    },
    DATETIME {

        @Override
        public void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry, final Record record)
                throws SQLException {
            statement.setDate(index,
                    ofNullable(record.getDateTime(entry.getName())).map(d -> Date.valueOf(d.toLocalDate())).orElse(null));
        }
    };

    public abstract void setValue(final PreparedStatement statement, final int index, final Schema.Entry entry,
            final Record record) throws SQLException;

    private static boolean hasKey(final Record record, final String name) {
        return record.getSchema().getEntries().stream().anyMatch(e -> e.getName().equals(name));
    }
}
