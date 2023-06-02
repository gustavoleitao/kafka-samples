package br.ufrn.imd.lii.kafka;

import br.ufrn.imd.lii.kafka.consumer.ConsumerService;
import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.consumer.ServiceRunner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.regex.Pattern;


public class KafkaSimpleConsumer implements ConsumerService<String> {

    private Logger logger = LoggerFactory.getLogger(KafkaSimpleStringProducer.class);

    @Override
    public Pattern topic() {
        return Pattern.compile("TOPIC_STRING_SAMPLE");
    }

    @Override
    public String consumerGroup() {
        return KafkaSimpleConsumer.class.getSimpleName() + "2";
    }

    @Override
    public void parse(ConsumerRecord<String, Message<String>> record) {
        System.out.println("Mensagem recebida. Chave="+ record.key() + " valor="+record.value() + " partição="+record.partition() + " offset="+record.offset());
    }

    public static void main(String[] args) throws Exception {
        new ServiceRunner<>(KafkaSimpleConsumer::new).start(1);
    }

}
