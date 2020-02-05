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
package org.talend.components.extension.api.virtual;

import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

import java.lang.annotation.Repeatable;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

// note: this configuration can become way more complex since it can also enable to override part of the configuration!
// for now it is mainly an example of extension
@Target(TYPE) // actually only @PartitionMapper or @Emitter types
@Retention(RUNTIME)
@Repeatable(VirtualChain.List.class)
public @interface VirtualChain {

    /**
     * @return List of processor types to chain this mapper/input with. Note it must be flatmap processors (single output).
     */
    Class<?>[] followedBy() default {};

    /**
     * @return the name of the virtual component in the family.
     */
    String name();

    /**
     * @return the icon of the virtual component in the family.
     */
    String icon();

    /**
     * @return should original source be removed from the active components.
     */
    boolean requiresToVetoParent() default true;

    // todo: add conditions on the configuration for example, using configuration path=value
    // KeyValue[] conditions() default {};

    @Target(TYPE)
    @Retention(RUNTIME)
    @interface List {

        VirtualChain[] value();
    }
}
