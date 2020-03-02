package br.ufrn.imd.lii.kafka.consumer;

import br.ufrn.imd.lii.kafka.consumer.ConsumerService;

public interface ServiceFactory<T> {

   ConsumerService<T> create();

}
