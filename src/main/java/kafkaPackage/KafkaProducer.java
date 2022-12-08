package kafkaPackage;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {
    private static final Producer<String, String> producer = createProducer();
    private static final String topic = "Measurements";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

    private static Producer<String, String> createProducer() {
        Properties props = new Properties();
        props.put("acks", "all");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new org.apache.kafka.clients.producer.KafkaProducer<>(props);
    }


    public static void sendPathData(String name, String measurements) {
        try {
            //System.out.println("name:measurement= " + name + ":" + measurements);
            final ProducerRecord<String, String> record =
                    new ProducerRecord<>(topic, name, measurements);
            producer.send(record, (metadata, exception) -> {
            });

        } finally {
            producer.flush();
        }
    }
}