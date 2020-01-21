package org.talend.components.pubsub.output.message;

import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import org.talend.components.pubsub.dataset.PubSubDataSet;
import org.talend.sdk.component.api.record.Record;

import javax.json.JsonObject;

public class JSONMessageGenerator extends MessageGenerator {


    @Override
    public void init(PubSubDataSet dataset) {

    }

    @Override
    public PubsubMessage generateMessage(Record record) {
        // TODO
        return PubsubMessage.newBuilder()
                .setData(ByteString.copyFromUtf8("json"))
                .build();
    }

    @Override
    public boolean acceptFormat(PubSubDataSet.ValueFormat format) {
        return format == PubSubDataSet.ValueFormat.JSON;
    }
}
