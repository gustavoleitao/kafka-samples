package br.ufrn.imd.lii.kafka;

import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.consumer.ConsumerService;
import br.ufrn.imd.lii.kafka.consumer.KafkaService;
import br.ufrn.imd.lii.kafka.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.regex.Pattern;

public class KafkaPatternConsumer implements ConsumerService<String> {

    private Logger logger = LoggerFactory.getLogger(KafkaPatternConsumer.class);

    @Override
    public Pattern topic() {
        return Pattern.compile("TOPIC_.*");
    }

    @Override
    public String consumerGroup() {
        return KafkaPatternConsumer.class.getSimpleName();
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
        logger.info("Mensgaem recebida.  chave="+ record.key() + " valor="+record.value().getPayload() + " partição="+record.partition() + " offset="+record.offset());
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        var runner = new ServiceRunner<>(KafkaPatternConsumer::new);
        runner.start(5);
    }

}
