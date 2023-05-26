package br.ufrn.imd.lii.kafka.consumer;

public interface ServiceFactory<T> {

   ConsumerService<T> create();

}
