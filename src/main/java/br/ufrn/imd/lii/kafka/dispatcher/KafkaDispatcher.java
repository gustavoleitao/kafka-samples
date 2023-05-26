package br.ufrn.imd.lii.kafka.dispatcher;

import br.ufrn.imd.lii.kafka.common.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, Message<T>> producer;

    public KafkaDispatcher() {
        this.producer = new KafkaProducer<>(properties());
    }

    public void send(String topic, String key, Message<T> value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value);
        producer.send(record, (data, ex) ->{
            if (ex != null){
                ex.printStackTrace();
                return;
            }
            System.out.println("Sucesso ao enviar dado. topico="+ data.topic() + " - partição=" + data.partition() + " offset="+data.offset() + " timestamp="+data.timestamp());
        }).get();
    }

    private Properties properties() {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, GsonSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
