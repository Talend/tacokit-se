package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.service.UIActionServiceTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("UIActionService  - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyUIActionServiceTest extends UIActionServiceTest implements DerbyTestInstance {

}
