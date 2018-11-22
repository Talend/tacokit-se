package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.UpdateTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output update  - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresqlUpdateTest extends UpdateTest implements PostgresqlTestInstance {

}
