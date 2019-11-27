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
package org.talend.components.extension;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Mark a Datastore as a reference coming from a datastore provider
 */
@Target({ FIELD })
@Retention(RUNTIME)
public @interface DatastoreRef {

    /**
     * This configurationId will be used to filter on the connections/datastores which will be fetched from the
     * datastore provider.
     *
     * @return the id of the configuration this DatastoreRef should filter on
     */
    String configurationId();

    /**
     * Filter datastore references
     */
    Filter[] filters() default {};

    @interface Filter {

        /**
         * key to filter
         */
        String key();

        /**
         * value of the {@link #key()}
         */
        String value();
    }

}
