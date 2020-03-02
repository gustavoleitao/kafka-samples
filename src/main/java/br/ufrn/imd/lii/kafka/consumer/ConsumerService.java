package br.ufrn.imd.lii.kafka.consumer;

import br.ufrn.imd.lii.kafka.common.Message;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.regex.Pattern;

public interface ConsumerService<T> {

    Pattern topic();

    String consumerGroup();

    void parse (ConsumerRecord<String, Message<T>> record);


}
