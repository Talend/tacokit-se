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

import java.util.List;

import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.ParserATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.tree.ParseTreeListener;
import org.antlr.v4.runtime.tree.TerminalNode;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
public class SoqlParser extends Parser {

    static {
        RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION);
    }

    protected static final DFA[] _decisionToDFA;

    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();

    public static final int WS = 1, COMMA = 2, DOT = 3, LPAR = 4, RPAR = 5, FROM = 6, SELECT = 7, NAME = 8, ANYCHAR = 9, BY = 10;

    public static final int RULE_query = 0, RULE_selectClause = 1, RULE_fromClause = 2, RULE_anythingClause = 3,
            RULE_fieldList = 4, RULE_subqueryList = 5, RULE_field = 6, RULE_object = 7, RULE_objectPrefix = 8, RULE_subquery = 9,
            RULE_subSelectClause = 10, RULE_anyword = 11, RULE_anything = 12;

    public static final String[] ruleNames = { "query", "selectClause", "fromClause", "anythingClause", "fieldList",
            "subqueryList", "field", "object", "objectPrefix", "subquery", "subSelectClause", "anyword", "anything" };

    private static final String[] _LITERAL_NAMES = { null, null, "','", "'.'", "'('", "')'" };

    private static final String[] _SYMBOLIC_NAMES = { null, "WS", "COMMA", "DOT", "LPAR", "RPAR", "FROM", "SELECT", "NAME",
            "ANYCHAR", "BY" };

    public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

    /**
     * @deprecated Use {@link #VOCABULARY} instead.
     */
    @Deprecated
    public static final String[] tokenNames;
    static {
        tokenNames = new String[_SYMBOLIC_NAMES.length];
        for (int i = 0; i < tokenNames.length; i++) {
            tokenNames[i] = VOCABULARY.getLiteralName(i);
            if (tokenNames[i] == null) {
                tokenNames[i] = VOCABULARY.getSymbolicName(i);
            }

            if (tokenNames[i] == null) {
                tokenNames[i] = "<INVALID>";
            }
        }
    }

    @Override
    @Deprecated
    public String[] getTokenNames() {
        return tokenNames;
    }

    @Override

    public Vocabulary getVocabulary() {
        return VOCABULARY;
    }

    @Override
    public String getGrammarFileName() {
        return "Soql.g4";
    }

    @Override
    public String[] getRuleNames() {
        return ruleNames;
    }

    @Override
    public String getSerializedATN() {
        return _serializedATN;
    }

    @Override
    public ATN getATN() {
        return _ATN;
    }

    public SoqlParser(TokenStream input) {
        super(input);
        _interp = new ParserATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
        removeErrorListeners();
        addErrorListener(new SoqlErrorListener());
    }

    public static class QueryContext extends ParserRuleContext {

        public SelectClauseContext selectClause() {
            return getRuleContext(SelectClauseContext.class, 0);
        }

        public FromClauseContext fromClause() {
            return getRuleContext(FromClauseContext.class, 0);
        }

        public AnythingClauseContext anythingClause() {
            return getRuleContext(AnythingClauseContext.class, 0);
        }

        public QueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_query;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterQuery(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitQuery(this);
        }
    }

    public final QueryContext query() throws RecognitionException {
        QueryContext _localctx = new QueryContext(_ctx, getState());
        enterRule(_localctx, 0, RULE_query);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(26);
                selectClause();
                setState(27);
                fromClause();
                setState(29);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << COMMA) | (1L << DOT) | (1L << LPAR) | (1L << RPAR)
                        | (1L << NAME) | (1L << ANYCHAR) | (1L << BY))) != 0)) {
                    {
                        setState(28);
                        anythingClause();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SelectClauseContext extends ParserRuleContext {

        public TerminalNode SELECT() {
            return getToken(SoqlParser.SELECT, 0);
        }

        public FieldListContext fieldList() {
            return getRuleContext(FieldListContext.class, 0);
        }

        public TerminalNode COMMA() {
            return getToken(SoqlParser.COMMA, 0);
        }

        public SubqueryListContext subqueryList() {
            return getRuleContext(SubqueryListContext.class, 0);
        }

        public SelectClauseContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_selectClause;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterSelectClause(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitSelectClause(this);
        }
    }

    public final SelectClauseContext selectClause() throws RecognitionException {
        SelectClauseContext _localctx = new SelectClauseContext(_ctx, getState());
        enterRule(_localctx, 2, RULE_selectClause);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(31);
                match(SELECT);
                setState(32);
                fieldList();
                setState(35);
                _errHandler.sync(this);
                _la = _input.LA(1);
                if (_la == COMMA) {
                    {
                        setState(33);
                        match(COMMA);
                        setState(34);
                        subqueryList();
                    }
                }

            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class FromClauseContext extends ParserRuleContext {

        public TerminalNode FROM() {
            return getToken(SoqlParser.FROM, 0);
        }

        public ObjectContext object() {
            return getRuleContext(ObjectContext.class, 0);
        }

        public FromClauseContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_fromClause;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterFromClause(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitFromClause(this);
        }
    }

    public final FromClauseContext fromClause() throws RecognitionException {
        FromClauseContext _localctx = new FromClauseContext(_ctx, getState());
        enterRule(_localctx, 4, RULE_fromClause);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(37);
                match(FROM);
                setState(38);
                object();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class AnythingClauseContext extends ParserRuleContext {

        public List<AnythingContext> anything() {
            return getRuleContexts(AnythingContext.class);
        }

        public AnythingContext anything(int i) {
            return getRuleContext(AnythingContext.class, i);
        }

        public AnythingClauseContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_anythingClause;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterAnythingClause(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitAnythingClause(this);
        }
    }

    public final AnythingClauseContext anythingClause() throws RecognitionException {
        AnythingClauseContext _localctx = new AnythingClauseContext(_ctx, getState());
        enterRule(_localctx, 6, RULE_anythingClause);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(41);
                _errHandler.sync(this);
                _la = _input.LA(1);
                do {
                    {
                        {
                            setState(40);
                            anything();
                        }
                    }
                    setState(43);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                } while ((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << COMMA) | (1L << DOT) | (1L << LPAR) | (1L << RPAR)
                        | (1L << NAME) | (1L << ANYCHAR) | (1L << BY))) != 0));
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class FieldListContext extends ParserRuleContext {

        public List<FieldContext> field() {
            return getRuleContexts(FieldContext.class);
        }

        public FieldContext field(int i) {
            return getRuleContext(FieldContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(SoqlParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(SoqlParser.COMMA, i);
        }

        public FieldListContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_fieldList;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterFieldList(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitFieldList(this);
        }
    }

    public final FieldListContext fieldList() throws RecognitionException {
        FieldListContext _localctx = new FieldListContext(_ctx, getState());
        enterRule(_localctx, 8, RULE_fieldList);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(45);
                field();
                setState(50);
                _errHandler.sync(this);
                _alt = getInterpreter().adaptivePredict(_input, 3, _ctx);
                while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER) {
                    if (_alt == 1) {
                        {
                            {
                                setState(46);
                                match(COMMA);
                                setState(47);
                                field();
                            }
                        }
                    }
                    setState(52);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 3, _ctx);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SubqueryListContext extends ParserRuleContext {

        public List<SubqueryContext> subquery() {
            return getRuleContexts(SubqueryContext.class);
        }

        public SubqueryContext subquery(int i) {
            return getRuleContext(SubqueryContext.class, i);
        }

        public List<TerminalNode> COMMA() {
            return getTokens(SoqlParser.COMMA);
        }

        public TerminalNode COMMA(int i) {
            return getToken(SoqlParser.COMMA, i);
        }

        public SubqueryListContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_subqueryList;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterSubqueryList(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitSubqueryList(this);
        }
    }

    public final SubqueryListContext subqueryList() throws RecognitionException {
        SubqueryListContext _localctx = new SubqueryListContext(_ctx, getState());
        enterRule(_localctx, 10, RULE_subqueryList);
        int _la;
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(53);
                subquery();
                setState(58);
                _errHandler.sync(this);
                _la = _input.LA(1);
                while (_la == COMMA) {
                    {
                        {
                            setState(54);
                            match(COMMA);
                            setState(55);
                            subquery();
                        }
                    }
                    setState(60);
                    _errHandler.sync(this);
                    _la = _input.LA(1);
                }
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class FieldContext extends ParserRuleContext {

        public TerminalNode NAME() {
            return getToken(SoqlParser.NAME, 0);
        }

        public ObjectPrefixContext objectPrefix() {
            return getRuleContext(ObjectPrefixContext.class, 0);
        }

        public FieldContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_field;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterField(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitField(this);
        }
    }

    public final FieldContext field() throws RecognitionException {
        FieldContext _localctx = new FieldContext(_ctx, getState());
        enterRule(_localctx, 12, RULE_field);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(62);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 5, _ctx)) {
                case 1: {
                    setState(61);
                    objectPrefix();
                }
                    break;
                }
                setState(64);
                match(NAME);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ObjectContext extends ParserRuleContext {

        public TerminalNode NAME() {
            return getToken(SoqlParser.NAME, 0);
        }

        public ObjectPrefixContext objectPrefix() {
            return getRuleContext(ObjectPrefixContext.class, 0);
        }

        public ObjectContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_object;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterObject(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitObject(this);
        }
    }

    public final ObjectContext object() throws RecognitionException {
        ObjectContext _localctx = new ObjectContext(_ctx, getState());
        enterRule(_localctx, 14, RULE_object);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(67);
                _errHandler.sync(this);
                switch (getInterpreter().adaptivePredict(_input, 6, _ctx)) {
                case 1: {
                    setState(66);
                    objectPrefix();
                }
                    break;
                }
                setState(69);
                match(NAME);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class ObjectPrefixContext extends ParserRuleContext {

        public List<TerminalNode> NAME() {
            return getTokens(SoqlParser.NAME);
        }

        public TerminalNode NAME(int i) {
            return getToken(SoqlParser.NAME, i);
        }

        public List<TerminalNode> DOT() {
            return getTokens(SoqlParser.DOT);
        }

        public TerminalNode DOT(int i) {
            return getToken(SoqlParser.DOT, i);
        }

        public ObjectPrefixContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_objectPrefix;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterObjectPrefix(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitObjectPrefix(this);
        }
    }

    public final ObjectPrefixContext objectPrefix() throws RecognitionException {
        ObjectPrefixContext _localctx = new ObjectPrefixContext(_ctx, getState());
        enterRule(_localctx, 16, RULE_objectPrefix);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(73);
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1: {
                        {
                            setState(71);
                            match(NAME);
                            setState(72);
                            match(DOT);
                        }
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(75);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 7, _ctx);
                } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SubqueryContext extends ParserRuleContext {

        public TerminalNode LPAR() {
            return getToken(SoqlParser.LPAR, 0);
        }

        public SubSelectClauseContext subSelectClause() {
            return getRuleContext(SubSelectClauseContext.class, 0);
        }

        public FromClauseContext fromClause() {
            return getRuleContext(FromClauseContext.class, 0);
        }

        public TerminalNode RPAR() {
            return getToken(SoqlParser.RPAR, 0);
        }

        public SubqueryContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_subquery;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterSubquery(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitSubquery(this);
        }
    }

    public final SubqueryContext subquery() throws RecognitionException {
        SubqueryContext _localctx = new SubqueryContext(_ctx, getState());
        enterRule(_localctx, 18, RULE_subquery);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(77);
                match(LPAR);
                setState(78);
                subSelectClause();
                setState(79);
                fromClause();
                setState(80);
                match(RPAR);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class SubSelectClauseContext extends ParserRuleContext {

        public TerminalNode SELECT() {
            return getToken(SoqlParser.SELECT, 0);
        }

        public FieldListContext fieldList() {
            return getRuleContext(FieldListContext.class, 0);
        }

        public SubSelectClauseContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_subSelectClause;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterSubSelectClause(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitSubSelectClause(this);
        }
    }

    public final SubSelectClauseContext subSelectClause() throws RecognitionException {
        SubSelectClauseContext _localctx = new SubSelectClauseContext(_ctx, getState());
        enterRule(_localctx, 20, RULE_subSelectClause);
        try {
            enterOuterAlt(_localctx, 1);
            {
                setState(82);
                match(SELECT);
                setState(83);
                fieldList();
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class AnywordContext extends ParserRuleContext {

        public List<TerminalNode> ANYCHAR() {
            return getTokens(SoqlParser.ANYCHAR);
        }

        public TerminalNode ANYCHAR(int i) {
            return getToken(SoqlParser.ANYCHAR, i);
        }

        public AnywordContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_anyword;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterAnyword(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitAnyword(this);
        }
    }

    public final AnywordContext anyword() throws RecognitionException {
        AnywordContext _localctx = new AnywordContext(_ctx, getState());
        enterRule(_localctx, 22, RULE_anyword);
        try {
            int _alt;
            enterOuterAlt(_localctx, 1);
            {
                setState(86);
                _errHandler.sync(this);
                _alt = 1;
                do {
                    switch (_alt) {
                    case 1: {
                        {
                            setState(85);
                            match(ANYCHAR);
                        }
                    }
                        break;
                    default:
                        throw new NoViableAltException(this);
                    }
                    setState(88);
                    _errHandler.sync(this);
                    _alt = getInterpreter().adaptivePredict(_input, 8, _ctx);
                } while (_alt != 2 && _alt != org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static class AnythingContext extends ParserRuleContext {

        public TerminalNode NAME() {
            return getToken(SoqlParser.NAME, 0);
        }

        public AnywordContext anyword() {
            return getRuleContext(AnywordContext.class, 0);
        }

        public TerminalNode COMMA() {
            return getToken(SoqlParser.COMMA, 0);
        }

        public TerminalNode DOT() {
            return getToken(SoqlParser.DOT, 0);
        }

        public TerminalNode LPAR() {
            return getToken(SoqlParser.LPAR, 0);
        }

        public TerminalNode RPAR() {
            return getToken(SoqlParser.RPAR, 0);
        }

        public TerminalNode BY() {
            return getToken(SoqlParser.BY, 0);
        }

        public AnythingContext(ParserRuleContext parent, int invokingState) {
            super(parent, invokingState);
        }

        @Override
        public int getRuleIndex() {
            return RULE_anything;
        }

        @Override
        public void enterRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).enterAnything(this);
        }

        @Override
        public void exitRule(ParseTreeListener listener) {
            if (listener instanceof SoqlListener)
                ((SoqlListener) listener).exitAnything(this);
        }
    }

    public final AnythingContext anything() throws RecognitionException {
        AnythingContext _localctx = new AnythingContext(_ctx, getState());
        enterRule(_localctx, 24, RULE_anything);
        try {
            setState(97);
            _errHandler.sync(this);
            switch (_input.LA(1)) {
            case NAME:
                enterOuterAlt(_localctx, 1); {
                setState(90);
                match(NAME);
            }
                break;
            case ANYCHAR:
                enterOuterAlt(_localctx, 2); {
                setState(91);
                anyword();
            }
                break;
            case COMMA:
                enterOuterAlt(_localctx, 3); {
                setState(92);
                match(COMMA);
            }
                break;
            case DOT:
                enterOuterAlt(_localctx, 4); {
                setState(93);
                match(DOT);
            }
                break;
            case LPAR:
                enterOuterAlt(_localctx, 5); {
                setState(94);
                match(LPAR);
            }
                break;
            case RPAR:
                enterOuterAlt(_localctx, 6); {
                setState(95);
                match(RPAR);
            }
                break;
            case BY:
                enterOuterAlt(_localctx, 7); {
                setState(96);
                match(BY);
            }
                break;
            default:
                throw new NoViableAltException(this);
            }
        } catch (RecognitionException re) {
            _localctx.exception = re;
            _errHandler.reportError(this, re);
            _errHandler.recover(this, re);
        } finally {
            exitRule();
        }
        return _localctx;
    }

    public static final String _serializedATN = "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\3\ff\4\2\t\2\4\3\t"
            + "\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t\13\4"
            + "\f\t\f\4\r\t\r\4\16\t\16\3\2\3\2\3\2\5\2 \n\2\3\3\3\3\3\3\3\3\5\3&\n\3"
            + "\3\4\3\4\3\4\3\5\6\5,\n\5\r\5\16\5-\3\6\3\6\3\6\7\6\63\n\6\f\6\16\6\66"
            + "\13\6\3\7\3\7\3\7\7\7;\n\7\f\7\16\7>\13\7\3\b\5\bA\n\b\3\b\3\b\3\t\5\t"
            + "F\n\t\3\t\3\t\3\n\3\n\6\nL\n\n\r\n\16\nM\3\13\3\13\3\13\3\13\3\13\3\f"
            + "\3\f\3\f\3\r\6\rY\n\r\r\r\16\rZ\3\16\3\16\3\16\3\16\3\16\3\16\3\16\5\16"
            + "d\n\16\3\16\2\2\17\2\4\6\b\n\f\16\20\22\24\26\30\32\2\2g\2\34\3\2\2\2"
            + "\4!\3\2\2\2\6\'\3\2\2\2\b+\3\2\2\2\n/\3\2\2\2\f\67\3\2\2\2\16@\3\2\2\2"
            + "\20E\3\2\2\2\22K\3\2\2\2\24O\3\2\2\2\26T\3\2\2\2\30X\3\2\2\2\32c\3\2\2"
            + "\2\34\35\5\4\3\2\35\37\5\6\4\2\36 \5\b\5\2\37\36\3\2\2\2\37 \3\2\2\2 "
            + "\3\3\2\2\2!\"\7\t\2\2\"%\5\n\6\2#$\7\4\2\2$&\5\f\7\2%#\3\2\2\2%&\3\2\2"
            + "\2&\5\3\2\2\2\'(\7\b\2\2()\5\20\t\2)\7\3\2\2\2*,\5\32\16\2+*\3\2\2\2,"
            + "-\3\2\2\2-+\3\2\2\2-.\3\2\2\2.\t\3\2\2\2/\64\5\16\b\2\60\61\7\4\2\2\61"
            + "\63\5\16\b\2\62\60\3\2\2\2\63\66\3\2\2\2\64\62\3\2\2\2\64\65\3\2\2\2\65"
            + "\13\3\2\2\2\66\64\3\2\2\2\67<\5\24\13\289\7\4\2\29;\5\24\13\2:8\3\2\2"
            + "\2;>\3\2\2\2<:\3\2\2\2<=\3\2\2\2=\r\3\2\2\2><\3\2\2\2?A\5\22\n\2@?\3\2"
            + "\2\2@A\3\2\2\2AB\3\2\2\2BC\7\n\2\2C\17\3\2\2\2DF\5\22\n\2ED\3\2\2\2EF"
            + "\3\2\2\2FG\3\2\2\2GH\7\n\2\2H\21\3\2\2\2IJ\7\n\2\2JL\7\5\2\2KI\3\2\2\2"
            + "LM\3\2\2\2MK\3\2\2\2MN\3\2\2\2N\23\3\2\2\2OP\7\6\2\2PQ\5\26\f\2QR\5\6"
            + "\4\2RS\7\7\2\2S\25\3\2\2\2TU\7\t\2\2UV\5\n\6\2V\27\3\2\2\2WY\7\13\2\2"
            + "XW\3\2\2\2YZ\3\2\2\2ZX\3\2\2\2Z[\3\2\2\2[\31\3\2\2\2\\d\7\n\2\2]d\5\30"
            + "\r\2^d\7\4\2\2_d\7\5\2\2`d\7\6\2\2ad\7\7\2\2bd\7\f\2\2c\\\3\2\2\2c]\3"
            + "\2\2\2c^\3\2\2\2c_\3\2\2\2c`\3\2\2\2ca\3\2\2\2cb\3\2\2\2d\33\3\2\2\2\f" + "\37%-\64<@EMZc";

    public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}