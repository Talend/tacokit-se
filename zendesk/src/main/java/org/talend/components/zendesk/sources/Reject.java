package org.talend.components.zendesk.sources;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.json.JsonObject;
import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Reject implements Serializable {

    private String errorMessage;

    private JsonObject record;
}
