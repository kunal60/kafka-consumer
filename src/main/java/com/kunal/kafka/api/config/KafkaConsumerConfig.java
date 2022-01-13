package com.kunal.kafka.api.config;

import com.kunal.kafka.api.User;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.util.HashMap;
import java.util.Map;


@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    // config for String plain text

    // Here ConsumerFactory<String, String> contains String
    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        //Here we using StringDeserializer, because published message is in String
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "TravelSeries-1");
        //why we need the group above? There may be a chance , we consume Multiple Mediatype. Here we are specifying a String, so
        //I am specifying one groupId TravelSeries-1. For second publisher , i'll be consuming a raw object. So for that 'll provide a
        //different group Id
        return new DefaultKafkaConsumerFactory<>(configs);
    }

    //The consumerFactory() above, we need to inject it with KafkaContainerListener
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    // config for json data

    // Here ConsumerFactory<String, User> contains User
    @Bean
    public ConsumerFactory<String, User> userConsumerFactory() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        //Here we  not using StringDeserializer, because published message is in Json
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, "TravelSeries-2");
/*        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        //configs.put(JsonDeserializer.TRUSTED_PACKAGES, "your.package.name");
        configs.put(JsonDeserializer.TRUSTED_PACKAGES, "com.kunal.kafka.*");*/
        //In JsonDeserializer below, we passed the class, which we want to deserialize i.e. User
        return new DefaultKafkaConsumerFactory<>(configs, new StringDeserializer(), new JsonDeserializer<>(User.class));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, User> userKafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, User> factory = new ConcurrentKafkaListenerContainerFactory<String, User>();
        factory.setConsumerFactory(userConsumerFactory());
        return factory;
    }


}