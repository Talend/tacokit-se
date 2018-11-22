package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.QueryInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Query Input - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresqlQueryInputTest extends QueryInputTest implements PostgresqlTestInstance {

}
