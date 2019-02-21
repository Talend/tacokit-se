package org.talend.components.jdbc;

public @interface Disabled {

    Database value();

    /**
     * The reason this annotated test class or test method is disabled for this database.
     */
    String reason() default "Test disabled for this database";

}
