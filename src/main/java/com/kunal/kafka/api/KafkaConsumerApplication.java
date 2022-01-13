package com.kunal.kafka.api;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@SpringBootApplication
@RestController
public class KafkaConsumerApplication {

    List<String> messages = new ArrayList<>();

    User userFromTopic = null;

    //In Kafka, normally we shouldn't write these end points, as the @KafkaListener will take care. To display in UI, i write these Api's here
    @GetMapping("/consumeStringMessage")
    public List<String> consumeMsg() {
        return messages;
    }

    @GetMapping("/consumeJsonMessage")
    public User consumeJsonMessage() {
        return userFromTopic;
    }

    //Listener that will listen to String from topic
    //In containerFactory = "kafkaListenerContainerFactory" we are passing the bean name, which is bean Id here
    @KafkaListener(groupId = "TravelSeries-1", topics = "TravelSeries", containerFactory = "kafkaListenerContainerFactory")
    public List<String> getMsgFromTopic(String data) {
        System.out.println("GroupId TravelSeries-1 listened");
        messages.add(data);
        return messages;
    }

    //Listener that will listen to raw object from topic
    //In containerFactory = "userKafkaListenerContainerFactory" we are passing the bean name, which is bean Id here
    @KafkaListener(groupId = "TravelSeries-2", topics = "TravelSeries", containerFactory = "userKafkaListenerContainerFactory")
    public User getJsonMsgFromTopic(User user) {
        System.out.println("GroupId TravelSeries-2 listened");
        userFromTopic = user;
        return userFromTopic;
    }


    public static void main(String[] args) {
        SpringApplication.run(KafkaConsumerApplication.class, args);
    }

}
