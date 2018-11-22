package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.DeleteTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output delete  - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresqlDeleteTest extends DeleteTest implements PostgresqlTestInstance {

}
