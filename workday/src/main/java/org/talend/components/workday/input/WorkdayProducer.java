package org.talend.components.workday.input;

import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;
import org.talend.sdk.component.api.service.Service;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonValue;
import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Supplier;

@Emitter(family = "Workday", name = "Input")
public class WorkdayProducer implements Serializable {

    private final InputConfiguration inputConfig;

    @Service
    private transient WorkdayReaderService reader;

    private transient int offset = 0;

    private transient int limit = 300;

    private transient int total = -1;

    private transient Supplier<JsonObject> supplierObject;

    public WorkdayProducer(InputConfiguration inputConfig) {
        this.inputConfig = inputConfig;
    }

    @PostConstruct
    public void init() {
        BufferedProducerIterator<JsonObject> producerIterator = new BufferedProducerIterator<>(this::nextData);
        this.supplierObject = producerIterator::next;
    }

    @Producer
    public JsonObject next() {
        return supplierObject.get();
    }

    private Iterator<JsonObject> nextData() {

        JsonObject jsonRet = this.callNext();
        if (jsonRet == null) {
            return null;
        }
        JsonArray data = jsonRet.getJsonArray("data");
        return data.stream().map(JsonObject.class::cast).iterator();
    }

    private synchronized JsonObject callNext() {
        if (this.total >= 0 && this.offset >= this.total) {
            return null;
        }
        JsonObject ret = this.reader.find(this.inputConfig.getDataSet().getService(), this.offset, this.limit);
        this.total = ret.getInt("total");
        this.offset += limit;

        return ret;
    }


}
