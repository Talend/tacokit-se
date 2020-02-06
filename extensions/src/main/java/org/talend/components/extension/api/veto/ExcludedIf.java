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
package org.talend.components.extension.api.veto;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import org.talend.components.extension.api.KeyValue;

/**
 * Put on a component, it enables to not deploy the component under some conditions.
 *
 * IMPORTANT: at least one condition must be met (not all of them).
 */
@Target(TYPE)
@Retention(RUNTIME)
public @interface ExcludedIf {

    /**
     * @return the list of {@link org.talend.sdk.component.api.service.configuration.LocalConfiguration} properties
     * to evaluate for the deactivation of the marked component.
     */
    KeyValue[] configuration() default {};

    /**
     * IMPORTANT: don't use other conditional components there, ordering is not deterministic.
     *
     * @return the name of the components to check, if present, the component will be undeployed.
     */
    String[] componentExistsInTheFamily() default {};
}
