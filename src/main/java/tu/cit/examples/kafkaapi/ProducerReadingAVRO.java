package tu.cit.examples.kafkaapi;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class ProducerReadingAVRO {
    private static Logger logger = LogManager.getLogger();

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("schema.registry.url", "http://localhost:8081");

        KafkaProducer producer = new KafkaProducer(props);

        logger.info("Producer is created...");

        String key = "key1";

        String userSchema = "{\"type\":\"record\"," +
                "\"name\":\"myrecord\"," +
                "\"fields\":[{\"name\":\"f1\",\"type\":\"string\"}]}";
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(userSchema);
        GenericRecord avroRecord = new GenericData.Record(schema);
        for(int i=48;i<=50;i++) {
            avroRecord.put("f1", "value" +i);

            //ProducerRecord<String, Object> record = new ProducerRecord<>("topic1", key, avroRecord);
            ProducerRecord<String, GenericRecord> record = new ProducerRecord<>("topic-avro", key, avroRecord);
            try {
                producer.send(record,(RecordMetadata m, Exception e)->
                {
                    if (e !=null){
                        e.printStackTrace();
                    }else{
                        System.out.println("Message is produced at " + m.topic() + " : " + m.partition() + " : " + m.offset());
                    }
//                    public void onCompletion(RecordMetadata m, Exception e) {
//                    if(e != null) {
//                        e.printStackTrace();
//                    } else {
//                        logger.info("Message at offset : " + metadata.offset());
//                    }
//                }
                });
                logger.info("Message is produced  at " +record.partition() + " " +record.topic());
            } catch (SerializationException e) {
                // may need to do something with it
            }
// When you're finished producing records, you can flush the producer to ensure it has all been written to Kafka and
// then close the producer to free its resources.
//            finally {
//                producer.flush();
//                producer.close();
//            }
        }
        producer.flush();
        producer.close();
    }
}