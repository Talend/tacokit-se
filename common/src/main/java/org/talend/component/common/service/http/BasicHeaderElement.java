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
package org.talend.component.common.service.http;

import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class BasicHeaderElement implements HeaderElement, Cloneable {

    private final String name;

    private final String value;

    private final NameValuePair[] parameters;

    /**
     * Constructor with name, value and parameters.
     *
     * @param name header element name
     * @param value header element value. May be <tt>null</tt>
     * @param parameters header element parameters. May be <tt>null</tt>.
     * Parameters are copied by reference, not by value
     */
    public BasicHeaderElement(final String name, final String value, final NameValuePair[] parameters) {
        super();
        this.name = name;
        this.value = value;
        if (parameters != null) {
            this.parameters = parameters;
        } else {
            this.parameters = new NameValuePair[] {};
        }
    }

    /**
     * Constructor with name and value.
     *
     * @param name header element name
     * @param value header element value. May be <tt>null</tt>
     */
    public BasicHeaderElement(final String name, final String value) {
        this(name, value, null);
    }

    public String getName() {
        return this.name;
    }

    public String getValue() {
        return this.value;
    }

    public NameValuePair[] getParameters() {
        return this.parameters.clone();
    }

    public int getParameterCount() {
        return this.parameters.length;
    }

    public NameValuePair getParameter(final int index) {
        // ArrayIndexOutOfBoundsException is appropriate
        return this.parameters[index];
    }

    public NameValuePair getParameterByName(final String name) {
        NameValuePair found = null;
        if (name == null) {
            return found;
        }

        for (final NameValuePair current : this.parameters) {
            if (current.getName().equalsIgnoreCase(name)) {
                found = current;
                break;
            }
        }
        return found;
    }

    @Override
    public String toString() {
        final StringBuilder buffer = new StringBuilder();
        buffer.append(this.name);
        if (this.value != null) {
            buffer.append("=");
            buffer.append(this.value);
        }
        for (final NameValuePair parameter : this.parameters) {
            buffer.append("; ");
            buffer.append(parameter);
        }
        return buffer.toString();
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        // parameters array is considered immutable
        // no need to make a copy of it
        return super.clone();
    }

}
