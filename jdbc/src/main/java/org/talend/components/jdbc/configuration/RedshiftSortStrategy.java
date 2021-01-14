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
package org.talend.components.jdbc.configuration;

public enum RedshiftSortStrategy {

    /**
     * Don't apply any sort key
     */
    NONE,

    /**
     * When data is loaded into the table, the data is sorted by one or more columns that are designated as sort keys.
     * You can use the SORTKEY keyword after a column name to specify a single-column sort key
     */
    SINGLE,
    /**
     * Specifies that the data is sorted using a compound key made up of all of the listed columns, in the order they are listed.
     * A compound sort key is most useful when a query scans rows according to the order of the sort columns.
     * The performance benefits of sorting with a compound key decrease when queries rely on secondary sort columns.
     * You can define a maximum of 400 COMPOUND SORTKEY columns per table.
     */
    COMPOUND,

    /**
     * <p>
     * Specifies that the data is sorted using an interleaved sort key. A maximum of eight columns can be specified for an
     * interleaved sort key.
     * An interleaved sort gives equal weight to each column, or subset of columns, in the sort key, so queries do not depend on
     * the order of the columns in the sort key. When a query uses one or more secondary sort columns, interleaved sorting
     * significantly improves query performance.
     * Interleaved sorting carries a small overhead cost for data loading and vacuuming operations.
     * </p>
     * <p>
     * <b>Important</b>
     * Don’t use an interleaved sort key on columns with monotonically increasing attributes, such as identity columns, dates, or
     * timestamps.
     * </p>
     */
    INTERLEAVED
}
