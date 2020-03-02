package br.ufrn.imd.lii.kafka.common;

import java.util.UUID;

public class CorrelationId {

    private String uuid;

    public CorrelationId(String uuid) {
        this.uuid = uuid;
    }

    public static CorrelationId random(){
        return new CorrelationId(UUID.randomUUID().toString());
    }

    public CorrelationId continueWith(String title){
        return new CorrelationId(uuid + "-" + title);
    }

}
