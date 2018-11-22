package org.talend.components.jdbc.integration.postgres;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.service.UIActionServiceTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("UIActionService  - Postgresql")
@WithComponents(value = "org.talend.components.jdbc")
class PostgresqlUIActionServiceTest extends UIActionServiceTest implements PostgresqlTestInstance {

}
