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
package org.talend.components.common.text;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Convenient class to replace placeholders in a String giving a Function<String, String>.
 * Replace in String "${hello} ${world:-tdi}" with "hi tdi" with a function that convert 'hello' to 'hi'.
 * you choose delimiters (prefix / suffix) with limitations due to regexp usage :
 * suffix can't start with ':' or '-' (':-' separate key and default value. (can't translate "[:hello:]")
 * a word can't contains first symbole of a suffix, means that a suffix can't start with letter [Starthelloend] is not possible.
 * 
 * this class doesn't treat embraced substitution '${word${number}}' isn't translate to 'third'
 * with a function that give '3' for 'number' and 'third' for 'word3'
 * 
 * it doesn't treat recursive replacement: '${greeting}' with a function
 * 'greeting' -> '${hello} ${world:-tdi}" and 'hello' -> 'hi' will render '${hello} ${world:-tdi}'.
 */
public class Substitutor {

    private static final char ESCAPE = '\\';

    /** delimiter to separate a key and a default value or a variable ( ${key:-default} */
    private static final Pattern DELIMITER = Pattern.compile("([^:]*):-(.*)");

    /**
     * Define a result for a search in a string.
     */
    public static class FindResult {

        /** start position of a key in search string */
        public final int start;

        /** end position of a key in search string */
        public final int end;

        /** key finded */
        public final String key;

        public FindResult(int start, int end, String key) {
            this.start = start;
            this.end = end;
            this.key = key;
        }
    }

    /** key finder for a String */
    public static class KeyFinder {

        /** regular expression for search. */
        private final Pattern pattern;

        public KeyFinder(String prefix, String suffix) {
            String aPrefix = escapeChars(prefix);
            String aSuffix = escapeChars(suffix);
            String exp = aPrefix + "([^" + escapeChars(suffix.substring(0, 1)) + "]*)" + aSuffix;
            pattern = Pattern.compile(exp);
        }

        /**
         * Search all result to find in a string.
         * ex : "${hello} ${world:-tdi}" will produce 2 result as (0,8,hello), (9, 22, world:-tdi)
         * 
         * @param source
         * @return
         */
        public Iterator<FindResult> search(String source) {
            final Matcher matcher = pattern.matcher(source);
            return new Iterator<FindResult>() {

                private boolean hasNext = matcher.find();

                @Override
                public boolean hasNext() {
                    return this.hasNext;
                }

                @Override
                public FindResult next() {
                    if (hasNext) {
                        FindResult result = new FindResult(matcher.start(), matcher.end(), matcher.group(1));
                        this.hasNext = matcher.find();
                        return result;
                    }
                    return null;
                }
            };
        }

        /**
         * Refine prefix and suffix for regular expression.
         * 
         * @param val : prefix of suffix expression.
         * @return expression with reg exp compatibility.
         */
        private String escapeChars(String val) {
            val = val.replace("{", "\\{").replace("}", "\\}").replace("(", "\\(").replace(")", "\\)").replace("[", "\\[")
                    .replace("]", "\\]").replace("$", "\\$");

            return val;
        }
    }

    /** given place holder (dictionnary) */
    private final UnaryOperator<String> placeholderProvider;

    /** key finder with defined prefix / suffix */
    private final KeyFinder finder;

    /**
     * Constructor
     * 
     * @param finder : finder;
     * @param placeholderProvider Function used to replace the string
     */
    public Substitutor(KeyFinder finder, UnaryOperator<String> placeholderProvider) {
        this.finder = finder;
        if (placeholderProvider instanceof CachedPlaceHolder) {
            this.placeholderProvider = placeholderProvider;
        } else {
            this.placeholderProvider = new CachedPlaceHolder(placeholderProvider);
        }
    }

    public UnaryOperator<String> getPlaceholderProvider() {
        return placeholderProvider;
    }

    /**
     * Replace all the placeholders
     * 
     * @param source String to be parsed
     * @return new string with placeholders replaced
     */
    public String replace(final String source) {
        if (source == null) {
            return null;
        }

        final Iterator<FindResult> results = this.finder.search(source);

        StringBuilder sb = new StringBuilder();
        int curr = 0;
        while (results.hasNext()) {
            final FindResult result = results.next();

            if (result.start == 0 || source.charAt(result.start - 1) != ESCAPE) {
                sb.append(source.substring(curr, result.start)).append(findOrDefault(result.key));
                curr = result.end;
            } else { // escaped placeholder
                sb.append(source.substring(curr, result.start - 1));
                curr = result.start;
            }
        }

        if (curr < source.length()) {
            sb.append(source.substring(curr));
        }

        return sb.toString();
    }

    /**
     * Find value for key in place holder or give default.
     * 
     * @param key : simple key 'hello' or with default 'hello:-hi'
     * @return give value for key with function or default if it's unknown for function.
     */
    private String findOrDefault(String key) {
        String defaultValue = "";

        Matcher matcher = DELIMITER.matcher(key);
        if (matcher.matches()) {
            // there's a default value.
            key = matcher.group(1);
            defaultValue = matcher.group(2);
        }

        final String s = placeholderProvider.apply(key);
        return Optional.ofNullable(s).orElse(defaultValue);
    }

    /**
     * To optimized research of key.
     */
    static class CachedPlaceHolder implements UnaryOperator<String> {

        /** original place holder function. */
        private final UnaryOperator<String> originalFunction;

        /** cache for function */
        private final Map<String, Optional<String>> cache = new HashMap<>();

        public CachedPlaceHolder(UnaryOperator<String> originalFunction) {
            super();
            this.originalFunction = originalFunction;
        }

        @Override
        public String apply(String varName) {
            return cache.computeIfAbsent(varName, k -> Optional.ofNullable(originalFunction.apply(k))).orElse(null);
        }
    }
}
