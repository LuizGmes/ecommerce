package br.com.luiz.ecommerce;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Properties;
import java.util.regex.Pattern;

public class LogService {

    public static void main(String[] args) {
        LogService logService = new LogService();
        try(KafkaService service = new KafkaService(EmailService.class.getSimpleName(),
            Pattern.compile("ECOMMERCE.*"),
            logService::parse)) {
            service.run();
        }

    }

    private void parse(ConsumerRecord<String, String> record) {
        System.out.println("----------------------------------------");
        System.out.println("LOG: -----------------------------------");
        System.out.println("topic: "+ record.topic());
        System.out.println("key: "+ record.key());
        System.out.println("value: "+ record.value());
        System.out.println("partition: "+ record.partition());
        System.out.println("offset: "+ record.offset());
    }
}
