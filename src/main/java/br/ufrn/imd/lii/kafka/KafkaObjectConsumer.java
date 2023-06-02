package br.ufrn.imd.lii.kafka;

import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.consumer.ConsumerService;
import br.ufrn.imd.lii.kafka.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaObjectConsumer implements ConsumerService<KafkaSampleObject> {

    private Logger logger = LoggerFactory.getLogger(KafkaSimpleStringProducer.class);

    @Override
    public Pattern topic() {
        return Pattern.compile("TOPIC_OBJECT_SAMPLE");
    }

    @Override
    public String consumerGroup() {
        return KafkaObjectConsumer.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<KafkaSampleObject>> record) {
        System.out.println("Mensgaem recebida.  chave="+ record.key() + " valor="+record.value() + " partição="+record.partition() + " offset="+record.offset());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var runner = new ServiceRunner<>(KafkaObjectConsumer::new);
        runner.start(1);
    }

}
