package br.ufrn.imd.lii.kafka.dispatcher;

import br.ufrn.imd.lii.kafka.common.Message;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.Closeable;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaDispatcher<T> implements Closeable {

    private final KafkaProducer<String, T> producer;

    public KafkaDispatcher(Class<?> valueSerializer) {
        this.producer = new KafkaProducer<>(properties(valueSerializer));
    }

    public void send(String topic, String key, Message<T> value) throws ExecutionException, InterruptedException {
        var record = new ProducerRecord<>(topic, key, value.getPayload());
        Future<RecordMetadata> retorno = producer.send(record);

        try {
            var  metadata = retorno.get();
            System.out.println("Sucesso ao enviar dado. topico=" + metadata.topic() + " - partição=" + metadata.partition() + " offset=" + metadata.offset() + " timestamp=" + metadata.timestamp() + " message: " + value.getPayload());
        }catch (Exception ex){
            ex.printStackTrace();
        }

//        (data, ex) -> {
//            if (ex != null) {
//                ex.printStackTrace();
//                return;
//            }
//            System.out.println("Sucesso ao enviar dado. topico=" + data.topic() + " - partição=" + data.partition() + " offset=" + data.offset() + " timestamp=" + data.timestamp() + " message: " + value.getPayload());
//        }

    }

    private Properties properties(Class<?> valueSerializer) {
        var properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        return properties;
    }

    @Override
    public void close() {
        producer.close();
    }
}
