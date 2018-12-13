package org.talend.components.jdbc;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ ElementType.TYPE, ElementType.METHOD })
@Retention(RetentionPolicy.RUNTIME)
public @interface DisabledDatabases {

    /**
     * @return list of database that are disabled for this test class or method
     */
    Disabled[] value();

}
