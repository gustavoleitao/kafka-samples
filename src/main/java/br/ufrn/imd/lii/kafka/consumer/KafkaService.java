package br.ufrn.imd.lii.kafka.consumer;

import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.dispatcher.GsonSerializer;
import br.ufrn.imd.lii.kafka.dispatcher.KafkaDispatcher;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.io.Closeable;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;
import java.util.regex.Pattern;

public class KafkaService<T> implements Closeable {

    private Pattern topicPattern;
    private String groupId;
    private String topic;
    private KafkaConsumer<String, Message<T>> consumer;
    private Consumer<ConsumerRecord<String,Message<T>>> consumerFunction;
    private KafkaDispatcher<String> deadLetters = new KafkaDispatcher();

    public KafkaService(Pattern topicPattern, String groupId, Consumer<ConsumerRecord<String, Message<T>>> consumerFunction) {
        this.topicPattern = topicPattern;
        this.groupId = groupId;
        this.consumerFunction = consumerFunction;
    }

    public KafkaService(String topic, String groupId, Consumer<ConsumerRecord<String, Message<T>>> consumerFunction) {
        this.groupId = groupId;
        this.topic = topic;
        this.consumerFunction = consumerFunction;
    }

    private Properties properties(String groupId) {
        var properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //Máximo de registro para cada pool.
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1");
        //Novos consumidores, iniciarão consumo da ultima mensagem e não do inicio.
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); //earliest | latest
        return properties;
    }

    public void run() throws ExecutionException, InterruptedException {
        checkMandatoryParaments();
        consumer = new KafkaConsumer<>(properties(groupId));
        subscribe();
        while (true){
            var records = consumer.poll(Duration.ofMillis(100));
            if (!records.isEmpty()){
                for (var record : records){
                    try{
                        consumerFunction.accept(record);
                        consumer.commitSync();
                    }catch (Exception e){
                        var message = record.value();
                        deadLetters.send("DEAD_LETTER", message.getCorrelationId().toString(),
                                Message.of(message.getCorrelationId().continueWith("DeadLetter"), new String(new GsonSerializer().serialize("DEAD_LETTER", message))));
                    }
                }
            }
        }
    }

    private void checkMandatoryParaments() {
        if (groupId == null) throw new IllegalArgumentException("groupID deve ser diferente de nulo");
        if (consumerFunction == null) throw new IllegalArgumentException("consumerFunction deve ser diferente de nulo");
    }

    private void subscribe() {
        if (topic != null){
            consumer.subscribe(Arrays.asList(topic));
        }else if (topicPattern != null){
            consumer.subscribe(topicPattern);
        }
    }

    @Override
    public void close()  {
        consumer.close();
    }
}
