/*
 * Copyright (C) 2006-2020 Talend Inc. - www.talend.com
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
package org.talend.components.common.service.http.common;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Optional;

/**
 * Basic implementation for parsing header values into elements.
 * Instances of this class are stateless and thread-safe.
 * Derived classes are expected to maintain these properties.
 *
 * @since 4.0
 */
public class BasicHeaderValueParser {

    /**
     * A default instance of this class, for use as default or fallback.
     * Note that {@link BasicHeaderValueParser} is not a singleton, there
     * can be many instances of the class itself and of derived classes.
     * The instance here provides non-customized, default behavior.
     */
    public final static BasicHeaderValueParser DEFAULT = new BasicHeaderValueParser();

    private final static char PARAM_DELIMITER = ';';

    private final static char ELEM_DELIMITER = ',';

    private final static char[] ALL_DELIMITERS = new char[] { PARAM_DELIMITER, ELEM_DELIMITER };

    /**
     * Parses elements with the given parser.
     *
     * @param value the header value to parse
     * @param parser the parser to use, or <code>null</code> for default
     * @return array holding the header elements, never <code>null</code>
     */
    public final static BasicHeaderElement[] parseElements(final String value, BasicHeaderValueParser parser)
            throws ParseException {

        if (value == null) {
            throw new IllegalArgumentException("Value to parse may not be null");
        }

        if (parser == null)
            parser = BasicHeaderValueParser.DEFAULT;

        CharArrayBuffer buffer = new CharArrayBuffer(value.length());
        buffer.append(value);
        ParserCursor cursor = new ParserCursor(0, value.length());
        return parser.parseElements(buffer, cursor);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicHeaderElement[] parseElements(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException("Char array buffer may not be null");
        }
        if (cursor == null) {
            throw new IllegalArgumentException("Parser cursor may not be null");
        }

        List elements = new ArrayList();
        while (!cursor.atEnd()) {
            BasicHeaderElement element = parseHeaderElement(buffer, cursor);
            if (!(element.getName().length() == 0 && element.getValue() == null)) {
                elements.add(element);
            }
        }
        return (BasicHeaderElement[]) elements.toArray(new BasicHeaderElement[elements.size()]);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicHeaderElement parseHeaderElement(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException("Char array buffer may not be null");
        }
        if (cursor == null) {
            throw new IllegalArgumentException("Parser cursor may not be null");
        }

        BasicNameValuePair nvp = parseNameValuePair(buffer, cursor);
        BasicNameValuePair[] params = null;
        if (!cursor.atEnd()) {
            char ch = buffer.charAt(cursor.getPos() - 1);
            if (ch != ELEM_DELIMITER) {
                params = parseParameters(buffer, cursor);
            }
        }
        return createHeaderElement(nvp.getName(), nvp.getValue(), params);
    }

    /**
     * Creates a header element.
     * Called from {@link #parseHeaderElement}.
     *
     * @return a header element representing the argument
     */
    protected BasicHeaderElement createHeaderElement(final String name, final String value, final BasicNameValuePair[] params) {
        return new BasicHeaderElement(name, value, params);
    }

    public static Map<String, BasicNameValuePair> parseParametersAsMap(final BasicHeader header,
            final BasicHeaderValueParser parser) {
        BasicNameValuePair[] pairs = parseParameters(header, parser);
        Map<String, BasicNameValuePair> map = new HashMap<>();
        for (BasicNameValuePair p : pairs) {
            map.put(p.getName(), p);
        }

        return map;
    }

    public static BasicNameValuePair[] parseParameters(final BasicHeader header, final BasicHeaderValueParser parser) {

        BasicNameValuePair wwwAuthenticateType = null;
        String value = header.getValue().trim();

        boolean isWWWAuthent = "WWW-Authenticate".equals(header.getName());
        Optional<String> type = Optional.ofNullable(extractType(value));

        if (isWWWAuthent && type.isPresent()) {
            value = value.substring(type.get().length());
        }

        BasicNameValuePair[] nameValuePairs = parseParameters(value, parser);

        if (isWWWAuthent && type.isPresent()) {
            wwwAuthenticateType = new BasicNameValuePair("WWW-Authenticate-Type", type.get());
            nameValuePairs = Arrays.copyOf(nameValuePairs, nameValuePairs.length + 1);
            nameValuePairs[nameValuePairs.length - 1] = wwwAuthenticateType;
        }

        return nameValuePairs;
    }

    private static String extractType(String challenge) {
        char[] cc = challenge.trim().toCharArray();
        boolean space = false;
        boolean equal = false;
        StringBuilder type = new StringBuilder();
        for (char c : cc) {
            if (c == ' ') {
                space = true;
                break;
            }

            if (c == '=') {
                equal = true;
            }
            type.append(c);
        }

        if (space && !equal) {
            return type.toString();
        }

        return null;
    }

    /**
     * Parses parameters with the given parser.
     *
     * @param value the parameter list to parse
     * @param parser the parser to use, or <code>null</code> for default
     * @return array holding the parameters, never <code>null</code>
     */
    public final static BasicNameValuePair[] parseParameters(final String value, BasicHeaderValueParser parser)
            throws ParseException {

        if (value == null) {
            throw new IllegalArgumentException("Value to parse may not be null");
        }

        if (parser == null)
            parser = BasicHeaderValueParser.DEFAULT;

        CharArrayBuffer buffer = new CharArrayBuffer(value.length());
        buffer.append(value);
        ParserCursor cursor = new ParserCursor(0, value.length());
        return parser.parseParameters(buffer, cursor);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicNameValuePair[] parseParameters(final CharArrayBuffer buffer, final ParserCursor cursor) {

        if (buffer == null) {
            throw new IllegalArgumentException("Char array buffer may not be null");
        }
        if (cursor == null) {
            throw new IllegalArgumentException("Parser cursor may not be null");
        }

        int pos = cursor.getPos();
        int indexTo = cursor.getUpperBound();

        while (pos < indexTo) {
            char ch = buffer.charAt(pos);
            if (CharArrayBuffer.isWhitespace(ch)) {
                pos++;
            } else {
                break;
            }
        }
        cursor.updatePos(pos);
        if (cursor.atEnd()) {
            return new BasicNameValuePair[] {};
        }

        List params = new ArrayList();
        while (!cursor.atEnd()) {
            BasicNameValuePair param = parseNameValuePair(buffer, cursor);
            params.add(param);
        }

        return (BasicNameValuePair[]) params.toArray(new BasicNameValuePair[params.size()]);
    }

    // non-javadoc, see interface HeaderValueParser
    public BasicNameValuePair parseNameValuePair(final CharArrayBuffer buffer, final ParserCursor cursor) {
        return parseNameValuePair(buffer, cursor, ALL_DELIMITERS);
    }

    private static boolean isOneOf(final char ch, final char[] chs) {
        if (chs != null) {
            for (int i = 0; i < chs.length; i++) {
                if (ch == chs[i]) {
                    return true;
                }
            }
        }
        return false;
    }

    public BasicNameValuePair parseNameValuePair(final CharArrayBuffer buffer, final ParserCursor cursor,
            final char[] delimiters) {

        if (buffer == null) {
            throw new IllegalArgumentException("Char array buffer may not be null");
        }
        if (cursor == null) {
            throw new IllegalArgumentException("Parser cursor may not be null");
        }

        boolean terminated = false;

        int pos = cursor.getPos();
        int indexFrom = cursor.getPos();
        int indexTo = cursor.getUpperBound();

        // Find name
        String name = null;
        while (pos < indexTo) {
            char ch = buffer.charAt(pos);
            if (ch == '=') {
                break;
            }
            if (isOneOf(ch, delimiters)) {
                terminated = true;
                break;
            }
            pos++;
        }

        if (pos == indexTo) {
            terminated = true;
            name = buffer.substringTrimmed(indexFrom, indexTo);
        } else {
            name = buffer.substringTrimmed(indexFrom, pos);
            pos++;
        }

        if (terminated) {
            cursor.updatePos(pos);
            return createNameValuePair(name, null);
        }

        // Find value
        String value = null;
        int i1 = pos;

        boolean qouted = false;
        boolean escaped = false;
        while (pos < indexTo) {
            char ch = buffer.charAt(pos);
            if (ch == '"' && !escaped) {
                qouted = !qouted;
            }
            if (!qouted && !escaped && isOneOf(ch, delimiters)) {
                terminated = true;
                break;
            }
            if (escaped) {
                escaped = false;
            } else {
                escaped = qouted && ch == '\\';
            }
            pos++;
        }

        int i2 = pos;
        // Trim leading white spaces
        while (i1 < i2 && (CharArrayBuffer.isWhitespace(buffer.charAt(i1)))) {
            i1++;
        }
        // Trim trailing white spaces
        while ((i2 > i1) && (CharArrayBuffer.isWhitespace(buffer.charAt(i2 - 1)))) {
            i2--;
        }
        // Strip away quotes if necessary
        if (((i2 - i1) >= 2) && (buffer.charAt(i1) == '"') && (buffer.charAt(i2 - 1) == '"')) {
            i1++;
            i2--;
        }
        value = buffer.substring(i1, i2);
        if (terminated) {
            pos++;
        }
        cursor.updatePos(pos);
        return createNameValuePair(name, value);
    }

    /**
     * Creates a name-value pair.
     * Called from {@link #parseNameValuePair}.
     *
     * @param name the name
     * @param value the value, or <code>null</code>
     * @return a name-value pair representing the arguments
     */
    protected BasicNameValuePair createNameValuePair(final String name, final String value) {
        return new BasicNameValuePair(name, value);
    }

    public final static class ParseException extends RuntimeException {

    }

}
