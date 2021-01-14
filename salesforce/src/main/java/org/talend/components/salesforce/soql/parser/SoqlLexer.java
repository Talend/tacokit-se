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

import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.RuntimeMetaData;
import org.antlr.v4.runtime.Vocabulary;
import org.antlr.v4.runtime.VocabularyImpl;
import org.antlr.v4.runtime.atn.ATN;
import org.antlr.v4.runtime.atn.ATNDeserializer;
import org.antlr.v4.runtime.atn.LexerATNSimulator;
import org.antlr.v4.runtime.atn.PredictionContextCache;
import org.antlr.v4.runtime.dfa.DFA;

@SuppressWarnings({ "all", "warnings", "unchecked", "unused", "cast" })
public class SoqlLexer extends Lexer {

    static {
        RuntimeMetaData.checkVersion("4.6", RuntimeMetaData.VERSION);
    }

    protected static final DFA[] _decisionToDFA;

    protected static final PredictionContextCache _sharedContextCache = new PredictionContextCache();

    public static final int WS = 1, COMMA = 2, DOT = 3, LPAR = 4, RPAR = 5, FROM = 6, SELECT = 7, NAME = 8, ANYCHAR = 9;

    public static String[] modeNames = { "DEFAULT_MODE" };

    public static final String[] ruleNames = { "A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P",
            "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z", "WS", "COMMA", "DOT", "LPAR", "RPAR", "FROM", "SELECT", "NAME",
            "ANYCHAR" };

    private static final String[] _LITERAL_NAMES = { null, null, "','", "'.'", "'('", "')'" };

    private static final String[] _SYMBOLIC_NAMES = { null, "WS", "COMMA", "DOT", "LPAR", "RPAR", "FROM", "SELECT", "NAME",
            "ANYCHAR" };

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

    public SoqlLexer(CharStream input) {
        super(input);
        _interp = new LexerATNSimulator(this, _ATN, _decisionToDFA, _sharedContextCache);
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
    public String[] getModeNames() {
        return modeNames;
    }

    @Override
    public ATN getATN() {
        return _ATN;
    }

    public static final String _serializedATN = "\3\u0430\ud6d1\u8206\uad2d\u4417\uaef1\u8d80\uaadd\2\13\u00a1\b\1\4\2"
            + "\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"
            + "\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"
            + "\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"
            + "\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"
            + " \4!\t!\4\"\t\"\4#\t#\4$\t$\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3"
            + "\7\3\7\3\b\3\b\3\t\3\t\3\n\3\n\3\13\3\13\3\f\3\f\3\r\3\r\3\16\3\16\3\17"
            + "\3\17\3\20\3\20\3\21\3\21\3\22\3\22\3\23\3\23\3\24\3\24\3\25\3\25\3\26"
            + "\3\26\3\27\3\27\3\30\3\30\3\31\3\31\3\32\3\32\3\33\3\33\3\34\6\34\177"
            + "\n\34\r\34\16\34\u0080\3\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3"
            + "!\3!\3!\3!\3!\3\"\3\"\3\"\3\"\3\"\3\"\3\"\3#\3#\7#\u009b\n#\f#\16#\u009e"
            + "\13#\3$\3$\2\2%\3\2\5\2\7\2\t\2\13\2\r\2\17\2\21\2\23\2\25\2\27\2\31\2"
            + "\33\2\35\2\37\2!\2#\2%\2\'\2)\2+\2-\2/\2\61\2\63\2\65\2\67\39\4;\5=\6"
            + "?\7A\bC\tE\nG\13\3\2\37\4\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2GGgg\4\2HH"
            + "hh\4\2IIii\4\2JJjj\4\2KKkk\4\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2"
            + "QQqq\4\2RRrr\4\2SSss\4\2TTtt\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4"
            + "\2ZZzz\4\2[[{{\4\2\\\\||\5\2\13\f\17\17\"\"\6\2&&C\\aac|\7\2&&\62;C\\"
            + "aac|\u0088\2\67\3\2\2\2\29\3\2\2\2\2;\3\2\2\2\2=\3\2\2\2\2?\3\2\2\2\2"
            + "A\3\2\2\2\2C\3\2\2\2\2E\3\2\2\2\2G\3\2\2\2\3I\3\2\2\2\5K\3\2\2\2\7M\3"
            + "\2\2\2\tO\3\2\2\2\13Q\3\2\2\2\rS\3\2\2\2\17U\3\2\2\2\21W\3\2\2\2\23Y\3"
            + "\2\2\2\25[\3\2\2\2\27]\3\2\2\2\31_\3\2\2\2\33a\3\2\2\2\35c\3\2\2\2\37"
            + "e\3\2\2\2!g\3\2\2\2#i\3\2\2\2%k\3\2\2\2\'m\3\2\2\2)o\3\2\2\2+q\3\2\2\2"
            + "-s\3\2\2\2/u\3\2\2\2\61w\3\2\2\2\63y\3\2\2\2\65{\3\2\2\2\67~\3\2\2\29"
            + "\u0084\3\2\2\2;\u0086\3\2\2\2=\u0088\3\2\2\2?\u008a\3\2\2\2A\u008c\3\2"
            + "\2\2C\u0091\3\2\2\2E\u0098\3\2\2\2G\u009f\3\2\2\2IJ\t\2\2\2J\4\3\2\2\2"
            + "KL\t\3\2\2L\6\3\2\2\2MN\t\4\2\2N\b\3\2\2\2OP\t\5\2\2P\n\3\2\2\2QR\t\6"
            + "\2\2R\f\3\2\2\2ST\t\7\2\2T\16\3\2\2\2UV\t\b\2\2V\20\3\2\2\2WX\t\t\2\2"
            + "X\22\3\2\2\2YZ\t\n\2\2Z\24\3\2\2\2[\\\t\13\2\2\\\26\3\2\2\2]^\t\f\2\2"
            + "^\30\3\2\2\2_`\t\r\2\2`\32\3\2\2\2ab\t\16\2\2b\34\3\2\2\2cd\t\17\2\2d"
            + "\36\3\2\2\2ef\t\20\2\2f \3\2\2\2gh\t\21\2\2h\"\3\2\2\2ij\t\22\2\2j$\3"
            + "\2\2\2kl\t\23\2\2l&\3\2\2\2mn\t\24\2\2n(\3\2\2\2op\t\25\2\2p*\3\2\2\2"
            + "qr\t\26\2\2r,\3\2\2\2st\t\27\2\2t.\3\2\2\2uv\t\30\2\2v\60\3\2\2\2wx\t"
            + "\31\2\2x\62\3\2\2\2yz\t\32\2\2z\64\3\2\2\2{|\t\33\2\2|\66\3\2\2\2}\177"
            + "\t\34\2\2~}\3\2\2\2\177\u0080\3\2\2\2\u0080~\3\2\2\2\u0080\u0081\3\2\2"
            + "\2\u0081\u0082\3\2\2\2\u0082\u0083\b\34\2\2\u00838\3\2\2\2\u0084\u0085"
            + "\7.\2\2\u0085:\3\2\2\2\u0086\u0087\7\60\2\2\u0087<\3\2\2\2\u0088\u0089"
            + "\7*\2\2\u0089>\3\2\2\2\u008a\u008b\7+\2\2\u008b@\3\2\2\2\u008c\u008d\5"
            + "\r\7\2\u008d\u008e\5%\23\2\u008e\u008f\5\37\20\2\u008f\u0090\5\33\16\2"
            + "\u0090B\3\2\2\2\u0091\u0092\5\'\24\2\u0092\u0093\5\13\6\2\u0093\u0094"
            + "\5\31\r\2\u0094\u0095\5\13\6\2\u0095\u0096\5\7\4\2\u0096\u0097\5)\25\2"
            + "\u0097D\3\2\2\2\u0098\u009c\t\35\2\2\u0099\u009b\t\36\2\2\u009a\u0099"
            + "\3\2\2\2\u009b\u009e\3\2\2\2\u009c\u009a\3\2\2\2\u009c\u009d\3\2\2\2\u009d"
            + "F\3\2\2\2\u009e\u009c\3\2\2\2\u009f\u00a0\13\2\2\2\u00a0H\3\2\2\2\5\2" + "\u0080\u009c\3\2\3\2";

    public static final ATN _ATN = new ATNDeserializer().deserialize(_serializedATN.toCharArray());
    static {
        _decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
        for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
            _decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
        }
    }
}