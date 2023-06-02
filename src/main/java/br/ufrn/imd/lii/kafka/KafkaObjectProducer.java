package br.ufrn.imd.lii.kafka;

import br.ufrn.imd.lii.kafka.common.CorrelationId;
import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.dispatcher.GsonSerializer;
import br.ufrn.imd.lii.kafka.dispatcher.KafkaDispatcher;

import java.math.BigDecimal;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class KafkaObjectProducer {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        try (var producer = new KafkaDispatcher<KafkaSampleObject>(GsonSerializer.class)){

            KafkaSampleObject  object = KafkaSampleObject.builder()
                    .type(UUID.randomUUID().toString()).amount(new BigDecimal(Math.random() * 1_000)).build();

            producer.send("TOPIC_OBJECT_SAMPLE", UUID.randomUUID().toString(),
                    Message.of(CorrelationId.random(), object));
        }
    }
}
