package br.ufrn.imd.lii.kafka.dispatcher;

import br.ufrn.imd.lii.kafka.common.Message;
import br.ufrn.imd.lii.kafka.common.MessageAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

public class GsonSerializer<T> implements Serializer<T> {

    private Gson serializer = new GsonBuilder().registerTypeAdapter(Message.class, new MessageAdapter()).create();

    @Override
    public byte[] serialize(String topic, T object) {
        return serializer.toJson(object).getBytes();
    }

}
