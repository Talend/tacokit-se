package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.InsertTests;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output insert  - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresInsertTest extends InsertTests implements PostgresqlTestInstance {

}
