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

/**
 * Basic implementation for formatting header value elements.
 * Instances of this class are stateless and thread-safe.
 * Derived classes are expected to maintain these properties.
 *
 * @since 4.0
 */
public class BasicHeaderValueFormatter {

    /**
     * A default instance of this class, for use as default or fallback.
     * Note that {@link BasicHeaderValueFormatter} is not a singleton, there
     * can be many instances of the class itself and of derived classes.
     * The instance here provides non-customized, default behavior.
     *
     * @deprecated (4.3) use {@link #INSTANCE}
     */
    @Deprecated
    public final static BasicHeaderValueFormatter DEFAULT = new BasicHeaderValueFormatter();

    public final static BasicHeaderValueFormatter INSTANCE = new BasicHeaderValueFormatter();

    /**
     * Special characters that can be used as separators in HTTP parameters.
     * These special characters MUST be in a quoted string to be used within
     * a parameter value .
     */
    public final static String SEPARATORS = " ;,:@()<>\\\"/[]?={}\t";

    /**
     * Unsafe special characters that must be escaped using the backslash
     * character
     */
    public final static String UNSAFE_CHARS = "\"\\";

    // non-javadoc, see interface HeaderValueFormatter
    public CharArrayBuffer formatNameValuePair(final CharArrayBuffer charBuffer, final BasicNameValuePair nvp,
            final boolean quote) {
        final int len = estimateNameValuePairLen(nvp);
        CharArrayBuffer buffer = charBuffer;
        if (buffer == null) {
            buffer = new CharArrayBuffer(len);
        } else {
            buffer.ensureCapacity(len);
        }

        buffer.append(nvp.getName());
        final String value = nvp.getValue();
        if (value != null) {
            buffer.append('=');
            doFormatValue(buffer, value, quote);
        }

        return buffer;
    }

    /**
     * Estimates the length of a formatted name-value pair.
     *
     * @param nvp the name-value pair to format, or <code>null</code>
     *
     * @return a length estimate, in number of characters
     */
    protected int estimateNameValuePairLen(final BasicNameValuePair nvp) {
        if (nvp == null) {
            return 0;
        }

        int result = nvp.getName().length(); // name
        final String value = nvp.getValue();
        if (value != null) {
            // assume quotes, but no escaped characters
            result += 3 + value.length(); // ="value"
        }
        return result;
    }

    /**
     * Actually formats the value of a name-value pair.
     * This does not include a leading = character.
     * Called from {@link #formatNameValuePair formatNameValuePair}.
     *
     * @param buffer the buffer to append to, never <code>null</code>
     * @param value the value to append, never <code>null</code>
     * @param quote <code>true</code> to always format with quotes,
     * <code>false</code> to use quotes only when necessary
     */
    protected void doFormatValue(final CharArrayBuffer buffer, final String value, final boolean quote) {

        boolean quoteFlag = quote;
        if (!quoteFlag) {
            for (int i = 0; (i < value.length()) && !quoteFlag; i++) {
                quoteFlag = isSeparator(value.charAt(i));
            }
        }

        if (quoteFlag) {
            buffer.append('"');
        }
        for (int i = 0; i < value.length(); i++) {
            final char ch = value.charAt(i);
            if (isUnsafe(ch)) {
                buffer.append('\\');
            }
            buffer.append(ch);
        }
        if (quoteFlag) {
            buffer.append('"');
        }
    }

    /**
     * Checks whether a character is a {@link #SEPARATORS separator}.
     *
     * @param ch the character to check
     *
     * @return <code>true</code> if the character is a separator,
     * <code>false</code> otherwise
     */
    protected boolean isSeparator(final char ch) {
        return SEPARATORS.indexOf(ch) >= 0;
    }

    /**
     * Checks whether a character is {@link #UNSAFE_CHARS unsafe}.
     *
     * @param ch the character to check
     *
     * @return <code>true</code> if the character is unsafe,
     * <code>false</code> otherwise
     */
    protected boolean isUnsafe(final char ch) {
        return UNSAFE_CHARS.indexOf(ch) >= 0;
    }

}