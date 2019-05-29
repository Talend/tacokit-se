package org.talend.components.onedrive.sources;

import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.json.JsonObject;
import java.io.Serializable;

@Data
// @AllArgsConstructor
@NoArgsConstructor
public class Reject implements Serializable {

    @Setter
    private String errorMessage;

    @Setter
    private JsonObject record;
}
