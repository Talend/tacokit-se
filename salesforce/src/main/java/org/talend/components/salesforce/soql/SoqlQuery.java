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
package org.talend.components.salesforce.soql;

import java.util.ArrayList;
import java.util.List;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.tree.ParseTreeWalker;
import org.talend.components.salesforce.soql.parser.SoqlBaseListener;
import org.talend.components.salesforce.soql.parser.SoqlLexer;
import org.talend.components.salesforce.soql.parser.SoqlParser;
import org.talend.components.salesforce.soql.parser.SoqlParser.FieldContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.FieldListContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.FromClauseContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.ObjectContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.QueryContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.SelectClauseContext;
import org.talend.components.salesforce.soql.parser.SoqlParser.SubSelectClauseContext;

/**
 * Parses SOQL query and provides methods to access specific query parts
 */
public class SoqlQuery {

    /**
     * Parsed query tree
     */
    private QueryContext queryTree;

    /**
     * Driving (main) entity name
     */
    private String drivingEntityName;

    /**
     * List of field descriptions
     */
    private List<FieldDescription> fieldDescriptions;

    private static SoqlQuery soqlQueryRef;

    private SoqlQuery() {
    }

    public static SoqlQuery getInstance() {
        if (SoqlQuery.soqlQueryRef == null) {
            SoqlQuery.soqlQueryRef = new SoqlQuery();
        }
        return SoqlQuery.soqlQueryRef;
    }

    /**
     * init method parses input <code>queryString</code> and stores is as tree.
     * Should be invoked right after
     * {@link org.talend.components.salesforce.soql.SoqlQuery#getInstance()} method
     *
     * @param queryString SOQL query
     */
    public void init(String queryString) {
        ANTLRInputStream is = new ANTLRInputStream(queryString);
        SoqlLexer lexer = new SoqlLexer(is);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        SoqlParser parser = new SoqlParser(tokenStream);
        queryTree = parser.query();
        fieldDescriptions = new ArrayList<>();
        FieldRetrieverListener listener = new FieldRetrieverListener();
        ParseTreeWalker.DEFAULT.walk(listener, queryTree);
    }

    /**
     * Returns name of driving (main) entity E.g. in case of "SELECT (SELECT CreatedBy.Name FROM Notes), Name FROM
     * Account" query driving entity will be "Account"
     *
     * @return
     */
    public String getDrivingEntityName() {
        return drivingEntityName;
    }

    /**
     * Sets driving (main) entity name
     *
     * @param drivingEntityName entity name to set
     */
    private void setDrivingEntityName(String drivingEntityName) {
        this.drivingEntityName = drivingEntityName;
    }

    /**
     * Returns list of field descriptions from SOQL query
     *
     * @return list of collected field descriptions
     */
    public List<FieldDescription> getFieldDescriptions() {
        return fieldDescriptions;
    }

    /**
     * Listener which retrieves fields and corresponding SOQL objects (objects where particular field is stored) from
     * SOQL query
     */
    private class FieldRetrieverListener extends SoqlBaseListener {

        /**
         * Special part, which should be added for full colomn names if it comed from select subquery
         */
        private static final String RECORDS = "_records_";

        /**
         * Retrieves fields and SOQL objects from top level query (not including subqueries)
         */
        @Override
        public void enterQuery(SoqlParser.QueryContext queryContext) {
            SelectClauseContext selectClause = queryContext.selectClause();
            FieldListContext fieldList = selectClause.fieldList();
            List<FieldContext> fields = fieldList.field();

            // only one soql object is allowed in FROM clause
            FromClauseContext fromClause = queryContext.fromClause();
            ObjectContext soqlObject = fromClause.object();
            String entityName = soqlObject.getText();
            setDrivingEntityName(entityName);

            for (FieldContext field : fields) {
                String simpleName = buildSimpleName(field);
                String fullName = buildQueryFullName(entityName, field);
                List<String> entityNames = buildQueryEntityNames(entityName, field);

                FieldDescription fieldDescription = new FieldDescription(fullName, simpleName, entityNames);
                fieldDescriptions.add(fieldDescription);
            }
        }

        @Override
        public void enterSubquery(SoqlParser.SubqueryContext subQueryContext) {
            SubSelectClauseContext subSelectQuery = subQueryContext.subSelectClause();
            FieldListContext fieldList = subSelectQuery.fieldList();
            List<FieldContext> fields = fieldList.field();

            // only one soql object is allowed in FROM clause
            FromClauseContext fromClause = subQueryContext.fromClause();
            ObjectContext soqlObject = fromClause.object();
            String entityName = soqlObject.getText().replace(".", "_");

            for (FieldContext field : fields) {
                String simpleName = buildSimpleName(field);
                String fullName = buildSubqueryFullName(entityName, field);
                List<String> entityNames = buildSubqueryEntityNames(entityName, field);

                FieldDescription fieldDescription = new FieldDescription(fullName, simpleName, entityNames);
                fieldDescriptions.add(fieldDescription);
            }
        }

        private String buildSimpleName(FieldContext field) {
            String fieldName = field.getText();
            String[] splittedFieldNames = fieldName.split("\\.");
            return splittedFieldNames[splittedFieldNames.length - 1];
        }

        private String buildSubqueryFullName(String entityName, FieldContext field) {
            String fieldName = field.getText();
            String[] splittedFieldNames = fieldName.split("\\.");
            StringBuilder sb = new StringBuilder();
            sb.append(entityName);
            sb.append(RECORDS);
            for (int i = 0; i < splittedFieldNames.length; i++) {
                sb.append(splittedFieldNames[i]);
                sb.append("_");
            }
            sb.deleteCharAt(sb.length() - 1);
            return sb.toString();
        }

        private String buildQueryFullName(String entityName, FieldContext field) {
            return field.getText().replace(".", "_");
        }

        private List<String> buildSubqueryEntityNames(String entityName, FieldContext field) {
            String fieldName = field.getText();
            String[] splittedFieldNames = fieldName.split("\\.");
            List<String> entityNames = new ArrayList<>();
            entityNames.add(removeLastChar(entityName, 's'));
            for (int i = 0; i < splittedFieldNames.length - 1; i++) {
                entityNames.add(splittedFieldNames[i]);
            }
            return entityNames;
        }

        private List<String> buildQueryEntityNames(String entityName, FieldContext field) {
            String fieldName = field.getText();
            String[] splittedFieldNames = fieldName.split("\\.");
            List<String> entityNames = new ArrayList<>();
            entityNames.add(entityName);
            for (int i = 0; i < splittedFieldNames.length - 1; i++) {
                entityNames.add(splittedFieldNames[i]);
            }
            return entityNames;
        }

        /**
         * If <code>str</code> ends with char <code>c</code> then removes it <br>
         * If <code>str</code> doesn't end with char <code>c</code> then returns string without modification
         *
         * @param str input string
         * @param c char to be removed if str ends with it
         * @return string without specified char <code>c</code> in the end
         */
        private String removeLastChar(String str, char c) {
            int strLength = str.length();
            if (str.charAt(strLength - 1) == c) {
                str = str.substring(0, strLength - 1);
            }
            return str;
        }
    }
}