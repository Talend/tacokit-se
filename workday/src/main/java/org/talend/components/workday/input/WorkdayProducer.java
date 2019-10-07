package org.talend.components.workday.input;

import org.talend.components.workday.dataset.WorkdayDataSet;
import org.talend.components.workday.service.WorkdayReaderService;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.input.Emitter;
import org.talend.sdk.component.api.input.Producer;

import javax.annotation.PostConstruct;
import javax.json.JsonArray;
import javax.json.JsonObject;
import java.io.Serializable;
import java.util.Iterator;
import java.util.function.Supplier;

@Emitter(family = "Workday", name = "Input")
public class WorkdayProducer implements Serializable {

    private final InputConfiguration inputConfig;

    private transient final WorkdayReaderService reader;

    private transient final int limit = 100;

    private transient int total = -1;

    private transient Supplier<JsonObject> supplierObject;

    public WorkdayProducer(@Option("configuration") InputConfiguration inputConfig,
                           WorkdayReaderService reader) {
        this.inputConfig = inputConfig;
        this.reader = reader;
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

    private Iterator<JsonObject> nextData(int page) {

        JsonObject jsonRet = this.callNext(page);
        if (jsonRet == null) {
            return null;
        }
        JsonArray data = jsonRet.getJsonArray("data");
        return data.stream().map(JsonObject.class::cast).iterator();
    }

    private JsonObject callNext(int page) {
        if (this.total >= 0 && (page * this.limit) >= this.total) {
            return null;
        }
        WorkdayDataSet ds = this.inputConfig.getDataSet();
        JsonObject ret = this.reader.find(ds, (page * this.limit), this.limit);
        if (this.total < 0) {
            synchronized (this) {
                this.total = ret.getInt("total");
            }
        }
        return ret;
    }


}
