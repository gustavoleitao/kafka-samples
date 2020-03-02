package br.ufrn.imd.lii.kafka.consumer;

import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

public class ServiceRunner<T> {

    private final ServiceProvider<T> provider;

    private ExecutorService pool;

    public ServiceRunner(ServiceFactory<T> factory) {
        this.provider = new ServiceProvider<>(factory);
    }


    public void start(int threadCount){
        pool = Executors.newFixedThreadPool(threadCount);
        IntStream.rangeClosed(1,threadCount).forEach(index -> pool.submit(provider));
    }

    public void shutdown(Duration timeout) throws InterruptedException {
        if (pool != null){
            pool.shutdown();
            pool.awaitTermination(timeout.getSeconds(), TimeUnit.SECONDS);
        }
    }
}
