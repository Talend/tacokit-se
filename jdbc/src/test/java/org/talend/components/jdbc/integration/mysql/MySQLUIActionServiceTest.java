package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.service.UIActionServiceTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("UIActionService  - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLUIActionServiceTest extends UIActionServiceTest implements MySQLTestInstance {

}
