package org.talend.components.jdbc.derby;

import org.junit.jupiter.api.DisplayName;
import org.talend.components.jdbc.output.DeleteTest;
import org.talend.sdk.component.junit5.WithComponents;

@DisplayName("Output delete  - Derby")
@WithComponents(value = "org.talend.components.jdbc")
class DerbyDeleteTest extends DeleteTest implements DerbyTestInstance {

}
