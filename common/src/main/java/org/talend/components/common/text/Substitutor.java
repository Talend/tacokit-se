/*
 * Copyright (C) 2006-2019 Talend Inc. - www.talend.com
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Forked from commons-text but no need to bring a dep only for < 200 LOC
 * <p>
 * Convenient class to replace placeholders in a String giving a Function<String, String>.
 */
public class Substitutor {

    private final char escape = '\\';

    private final char[] delimiter = ":-".toCharArray();

    private char[] prefix;

    private char[] suffix;

    private final Function<String, String> placeholderProvider;

    private final Map<String, Optional<String>> cache = new HashMap<>();

    public Substitutor(final String prefix, final String suffix, Function<String, String> placeholderProvider) {
        this.prefix = prefix.toCharArray();
        this.suffix = suffix.toCharArray();
        this.placeholderProvider = placeholderProvider;
    }

    public Substitutor(final String prefix, final String suffix, final Map<String, Optional<String>> intialCache,
            Function<String, String> placeholderProvider) {
        this.prefix = prefix.toCharArray();
        this.suffix = suffix.toCharArray();
        this.placeholderProvider = placeholderProvider;
        this.cache.putAll(intialCache);
    }

    public void setPrefix(String prefix) {
        this.prefix = prefix.toCharArray();
    }

    public String getPrefix() {
        return new String(prefix);
    }

    public void setSuffix(String suffix) {
        this.suffix = suffix.toCharArray();
    }

    public String replace(final String source) {
        if (source == null) {
            return null;
        }
        final StringBuilder builder = new StringBuilder(source);
        if (substitute(builder, 0, source.length(), null) <= 0) {
            return source;
        }

        return builder.toString();
    }

    private int substitute(final StringBuilder buf, final int offset, final int length, List<String> priorVariables) {
        final boolean top = priorVariables == null;
        boolean altered = false;
        int lengthChange = 0;
        char[] chars = buf.toString().toCharArray();
        int bufEnd = offset + length;
        int pos = offset;
        while (pos < bufEnd) {
            final int startMatchLen = isMatch(prefix, chars, pos, bufEnd);
            if (startMatchLen == 0) {
                pos++;
            } else {
                if (pos > offset && chars[pos - 1] == escape) {
                    buf.deleteCharAt(pos - 1);
                    chars = buf.toString().toCharArray();
                    lengthChange--;
                    altered = true;
                    bufEnd--;
                } else {
                    final int startPos = pos;
                    pos += startMatchLen;
                    int endMatchLen;
                    while (pos < bufEnd) {
                        endMatchLen = isMatch(suffix, chars, pos, bufEnd);
                        if (endMatchLen == 0) {
                            pos++;
                        } else {
                            String varNameExpr = new String(chars, startPos + startMatchLen, pos - startPos - startMatchLen);
                            pos += endMatchLen;
                            final int endPos = pos;

                            String varName = varNameExpr;
                            String varDefaultValue = null;

                            final char[] varNameExprChars = varNameExpr.toCharArray();
                            for (int i = 0; i < varNameExprChars.length; i++) {
                                if (isMatch(prefix, varNameExprChars, i, varNameExprChars.length) != 0) {
                                    break;
                                }
                                final int match = isMatch(delimiter, varNameExprChars, i, varNameExprChars.length);
                                if (match != 0) {
                                    varName = varNameExpr.substring(0, i);
                                    varDefaultValue = varNameExpr.substring(i + match);
                                    break;
                                }
                            }

                            if (priorVariables == null) {
                                priorVariables = new ArrayList<>();
                                priorVariables.add(new String(chars, offset, length + lengthChange));
                            }

                            checkCyclicSubstitution(varName, priorVariables);
                            priorVariables.add(varName);

                            final String varValue = getOrDefault(varName, varDefaultValue);
                            if (varValue != null) {
                                final int varLen = varValue.length();
                                buf.replace(startPos, endPos, varValue);
                                altered = true;
                                int change = substitute(buf, startPos, varLen, priorVariables);
                                change = change + varLen - (endPos - startPos);
                                pos += change;
                                bufEnd += change;
                                lengthChange += change;
                                chars = buf.toString().toCharArray();
                            }

                            priorVariables.remove(priorVariables.size() - 1);
                            break;
                        }
                    }
                }
            }
        }
        if (top) {
            return altered ? 1 : 0;
        }
        return lengthChange;
    }

    protected String getOrDefault(final String varName, final String varDefaultValue) {
        return cache.computeIfAbsent(varName, k -> Optional.ofNullable(placeholderProvider.apply(k))).orElse(varDefaultValue);
    }

    private int isMatch(final char[] chars, final char[] buffer, int pos, final int bufferEnd) {
        final int len = chars.length;
        if (pos + len > bufferEnd) {
            return 0;
        }
        for (int i = 0; i < chars.length; i++, pos++) {
            if (chars[i] != buffer[pos]) {
                return 0;
            }
        }
        return len;
    }

    private void checkCyclicSubstitution(final String varName, final List<String> priorVariables) {
        if (!priorVariables.contains(varName)) {
            return;
        }
        final StringBuilder buf = new StringBuilder(256);
        buf.append("Infinite loop in property interpolation of ");
        buf.append(priorVariables.remove(0));
        buf.append(": ");
        appendWithSeparators(buf, priorVariables);
        throw new IllegalStateException(buf.toString());
    }

    private void appendWithSeparators(final StringBuilder builder, final Collection<String> iterable) {
        if (iterable != null && !iterable.isEmpty()) {
            final Iterator<?> it = iterable.iterator();
            while (it.hasNext()) {
                builder.append(it.next());
                if (it.hasNext()) {
                    builder.append("->");
                }
            }
        }
    }

    public Map<String, Optional<String>> getCache() {
        return cache.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

}
