package br.ufrn.imd.lii.kafka;

import br.ufrn.imd.lii.kafka.common.CorrelationId;
import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.dispatcher.KafkaDispatcher;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaSimpleStringProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaDispatcher<String>()){
            producer.send("TOPIC_STRING_SAMPLE", UUID.randomUUID().toString(), Message.of(CorrelationId.random(), "VALUE"));
        }
    }
}
