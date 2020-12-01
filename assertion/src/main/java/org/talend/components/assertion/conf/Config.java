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
package org.talend.components.assertion.conf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.talend.components.assertion.service.DateValidator;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.configuration.constraint.Required;
import org.talend.sdk.component.api.configuration.type.DataStore;
import org.talend.sdk.component.api.configuration.ui.DefaultValue;
import org.talend.sdk.component.api.configuration.ui.OptionsOrder;
import org.talend.sdk.component.api.configuration.ui.layout.GridLayout;
import org.talend.sdk.component.api.meta.Documentation;
import org.talend.sdk.component.api.configuration.type.DataSet;
import org.talend.sdk.component.api.record.Schema;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

@Data
@GridLayout({ @GridLayout.Row({ "dse" }), @GridLayout.Row({ "dateFormat" }), @GridLayout.Row({ "dieOnError" }),
        @GridLayout.Row({ "assertions" }) })
public class Config implements Serializable {

    // @TODO : should remove datastore/dataset, the connector should be a simple processor
    @Option
    @Documentation("")
    AssertDSE dse;

    @Option
    @Documentation("")
    @DefaultValue(DateValidator.DEFAULT_DATE_FORMAT)
    String dateFormat = DateValidator.DEFAULT_DATE_FORMAT;

    @Option
    @Documentation("Throw a RuntimeException when an assertion fails or only log it.")
    boolean dieOnError = true;

    @Option
    @Required
    @Documentation("List of assertions.")
    List<AssertEntry> assertions;

    @GridLayout({ @GridLayout.Row({ "dso" }) })
    @Data
    @DataSet("assertion_dse")
    public static class AssertDSE implements Serializable {

        @Option
        @Documentation("")
        AssertDSO dso;

    }

    @GridLayout({})
    @DataStore("assertion_dso")
    public static class AssertDSO implements Serializable {

    }

    public void addAssertEntry(AssertEntry e) {
        if (this.assertions == null) {
            this.assertions = new ArrayList<>();
        }

        this.assertions.add(e);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @OptionsOrder({ "path", "type", "condition", "value", "err_message" })
    @Documentation("Assertion description entry.")
    public static class AssertEntry implements Serializable {

        @Option
        @Required
        @Documentation("Path of the element to test.")
        private String path;

        @Option
        @Required
        @Documentation("Check the expected type.")
        private Schema.Type type;

        @Option
        @Required
        @Documentation("Check the expected type.")
        private Condition condition;

        @Option
        @Required
        @Documentation("The expected value.")
        private String value;

        @Option
        @Required
        @Documentation("The error message")
        private String err_message;

        @Override
        public String toString() {
            return "* " + err_message + " : \n" + path + " of type " + type + " was tested on " + condition
                    + " with expected value '" + value + "'";
        }

    }

    public enum Condition {
        EQUALS,
        INFERIOR,
        SUPERIOR,
        CONTAINS,
        IS_NULL,
        CUSTOM
    }

}
