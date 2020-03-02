package br.ufrn.imd.lii.kafka.consumer;

import java.util.concurrent.Callable;

public class ServiceProvider<T> implements Callable<Void> {

    private final ServiceFactory<T> factory;

    public ServiceProvider(ServiceFactory<T> factory) {
        this.factory = factory;
    }

    @Override
    public Void call() throws Exception {
        var consumer = factory.create();
        try (KafkaService<T> service =
                     new KafkaService<>(consumer.topic(), consumer.consumerGroup(), consumer::parse)){
            service.run();
        }
        return null;
    }
}
