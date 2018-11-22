package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.UpdateTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output update  - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyUpdateTest extends UpdateTest implements DerbyTestInstance {

}
