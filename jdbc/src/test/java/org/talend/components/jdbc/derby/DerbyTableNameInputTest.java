package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.input.TableNameInputTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("TableName Input - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyTableNameInputTest extends TableNameInputTest implements DerbyTestInstance {

}
