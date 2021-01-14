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
package org.talend.components.salesforce.soql.parser;

import org.antlr.v4.runtime.tree.ParseTreeListener;

/**
 * This interface defines a complete listener for a parse tree produced by
 * {@link SoqlParser}.
 */
public interface SoqlListener extends ParseTreeListener {

    /**
     * Enter a parse tree produced by {@link SoqlParser#query}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterQuery(SoqlParser.QueryContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#query}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitQuery(SoqlParser.QueryContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#selectClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterSelectClause(SoqlParser.SelectClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#selectClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitSelectClause(SoqlParser.SelectClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#fromClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterFromClause(SoqlParser.FromClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#fromClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitFromClause(SoqlParser.FromClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#anythingClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterAnythingClause(SoqlParser.AnythingClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#anythingClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitAnythingClause(SoqlParser.AnythingClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#fieldList}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterFieldList(SoqlParser.FieldListContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#fieldList}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitFieldList(SoqlParser.FieldListContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#subqueryList}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterSubqueryList(SoqlParser.SubqueryListContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#subqueryList}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitSubqueryList(SoqlParser.SubqueryListContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#field}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterField(SoqlParser.FieldContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#field}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitField(SoqlParser.FieldContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#object}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterObject(SoqlParser.ObjectContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#object}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitObject(SoqlParser.ObjectContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#objectPrefix}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterObjectPrefix(SoqlParser.ObjectPrefixContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#objectPrefix}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitObjectPrefix(SoqlParser.ObjectPrefixContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#subquery}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterSubquery(SoqlParser.SubqueryContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#subquery}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitSubquery(SoqlParser.SubqueryContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#subSelectClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterSubSelectClause(SoqlParser.SubSelectClauseContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#subSelectClause}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitSubSelectClause(SoqlParser.SubSelectClauseContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#anyword}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterAnyword(SoqlParser.AnywordContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#anyword}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitAnyword(SoqlParser.AnywordContext ctx);

    /**
     * Enter a parse tree produced by {@link SoqlParser#anything}.
     * 
     * @param ctx
     * the parse tree
     */
    void enterAnything(SoqlParser.AnythingContext ctx);

    /**
     * Exit a parse tree produced by {@link SoqlParser#anything}.
     * 
     * @param ctx
     * the parse tree
     */
    void exitAnything(SoqlParser.AnythingContext ctx);
}