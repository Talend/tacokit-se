package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.QueryInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Query Input - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLQueryInputTest extends QueryInputTest implements MySQLTestInstance {

}
