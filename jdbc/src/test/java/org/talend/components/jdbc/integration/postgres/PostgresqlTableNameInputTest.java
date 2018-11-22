package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.TableNameInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("TableName Input - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresqlTableNameInputTest extends TableNameInputTest implements PostgresqlTestInstance {

}
