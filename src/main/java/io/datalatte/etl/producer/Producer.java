package io.datalatte.etl.producer;

import java.util.List;

public interface Producer<O> {
    void send(O o);
    void sendBatch(List<O> batch);
}
