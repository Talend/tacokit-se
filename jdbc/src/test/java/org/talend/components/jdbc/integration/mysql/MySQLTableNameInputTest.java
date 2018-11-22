package org.talend.components.jdbc.integration.mysql;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.TableNameInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("TableName Input - MySQL")
@WithComponents(value = "org.talend.components.jdbc")
class MySQLTableNameInputTest extends TableNameInputTest implements MySQLTestInstance {

}
