package br.ufrn.imd.lii.kafka.common;

import lombok.Data;

@Data
public class Message<T> {

    private CorrelationId correlationId;

    private T payload;

    public static <T> Message<T> of(CorrelationId id, T payload){
        return new Message<>(id, payload);
    }

    private Message(CorrelationId correlationId, T payload) {
        this.correlationId = correlationId;
        this.payload = payload;
    }

    @Override
    public String toString() {
        return "Message{" +
                "correlationId=" + correlationId +
                ", payload=" + payload +
                '}';
    }
}
