
package br.com.luiz.ecommerce;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

    public static void main(String [] args) throws ExecutionException, InterruptedException {
        KafkaProducer producer = new KafkaProducer<String, String>(properties());

        final Callback callback = (data, ex) -> {
            if (ex != null) {
                ex.printStackTrace();
                return;
            }
            System.out.println("sucesso enviando " + data.topic() + ":::partition" + data.partition() + "/ offset" + data.offset() + "/ timestamp" + data.timestamp());
        };

        String orderKey = "PED_1317 - teste 3";
        String orderValue = "132123,366954,1234";
        ProducerRecord orderRecord = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", orderKey, orderValue);
        producer.send(orderRecord, callback).get();

        String emailKey = "EMAIL_0005 -teste 3";
        String emailValue = "Thank you for your order, We are processing your order";
        ProducerRecord emailRecord = new ProducerRecord<String, String>("ECOMMERCE_SEND_EMAIL", emailKey, emailValue);
        producer.send(emailRecord, callback).get();
    }
    private static Properties properties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
}
