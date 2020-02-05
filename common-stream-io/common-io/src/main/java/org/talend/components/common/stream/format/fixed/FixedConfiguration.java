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
package org.talend.components.common.stream.format.fixed;

import org.talend.components.common.stream.format.ContentFormat;
import org.talend.components.common.stream.format.LineConfiguration;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Pattern;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;

import lombok.Data;

@Data
@GridLayout({ @GridLayout.Row("lineConfiguration"), @GridLayout.Row("lengthFields") })
public class FixedConfiguration implements ContentFormat {

    /** Serialization */
    private static final long serialVersionUID = -7116638194610358461L;

    /** constraint pattern for lengthFields (form "[number];[number]...> accepted */
    public static final String LengthPattern = "^([0-9]+;{0,1})*[0-9]+$";

    @Option
    @Documentation("line delimiter")
    private LineConfiguration lineConfiguration;

    @Option
    @Pattern(FixedConfiguration.LengthPattern)
    @Documentation("fields lengths separate by ';'")
    /** all fields length separate by ';' */
    private String lengthFields;

    /** fields length in table format */
    private int[] realLengthFields = new int[] {};

    /**
     * Redefine lengthFields setter to update realLengthFields.
     * 
     * @param lengthFields : new fields length string.
     */
    public void setLengthFields(String lengthFields) {
        this.lengthFields = lengthFields;
        if (this.isValid()) {
            realLengthFields = this.translateFields();
        } else {
            realLengthFields = new int[] {};
        }
    }

    /**
     * Test if lengthFields is valid.
     * 
     * @return true if ok.
     */
    public boolean isValid() {
        return this.lengthFields != null && this.lengthFields.matches(FixedConfiguration.LengthPattern);
    }

    /**
     * Get array of length from this.lengthFields ("23;12;3" -> [23, 12, 3])
     * 
     * @return array of length
     */
    private int[] translateFields() {
        String[] res = this.lengthFields.split(";");
        int[] lengths = new int[res.length];
        for (int i = 0; i < res.length; i++) {
            lengths[i] = Integer.parseInt(res[i]);
        }
        return lengths;
    }

}
