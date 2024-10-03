package org.example;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.UUID;
import java.util.Properties;
import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class SendToKafkaUsingSchema1 {
    static long from1, from2, to1, to2;

    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "kfk-dev-03.dmp.vimpelcom.ru:6667");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false debug=true keyTab=\"./tech_legalstage_ms.keytab\" principal=\"tech_legalstage_ms@BEE.VIMPELCOM.RU\" serviceName=\"kafka\";");
        props.put("schema.registry.url", "http://kfk-dev-01.dmp.vimpelcom.ru:7788/api/v1/confluent");

        props.put("key.serializer", org.apache.kafka.common.serialization.StringSerializer.class);
        props.put("value.serializer", io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        props.put("specific.avro.reader", "true");

        Producer<String, GenericRecord> producer = new KafkaProducer<>(props);

        final String topic = "legal_markcp_pers_dev";
        Schema schema = new Schema.Parser().parse(new File("AvroKafka.json"));
        System.out.println("schema: " + schema);
        from2 = System.currentTimeMillis();
        try (BufferedReader reader = new BufferedReader(new FileReader("./mark20000.csv"))) {
            String s = null;
            while ((s = reader.readLine()) != null) {
                String[] sS = s.split(";");
                GenericRecord genericRecord = new GenericData.Record(schema);
                genericRecord.put("ctn",           sS[0]);
                genericRecord.put("customer_id",   Long.parseLong(sS[1]));
                genericRecord.put("subscriber_id", Long.parseLong(sS[2]));
                genericRecord.put("indicator_id",  sS[3]);
                genericRecord.put("iw_value",      sS[4]);
                genericRecord.put("calc_date",     sS[5]);
                genericRecord.put("operation",     sS[6]);
                System.out.println("genericRecord: " + genericRecord);
                String uid = UUID.randomUUID().toString();
                System.out.println("uid: " + uid);

                ProducerRecord<String, GenericRecord> record = new ProducerRecord<>(topic, uid, genericRecord);
                System.out.println("record: " + record);
                from1 = System.currentTimeMillis();
                long startTime = System.nanoTime();
                RecordMetadata metadata = producer.send(record).get(); // blocking read
                long elapsedTime = System.nanoTime() - startTime;
                System.out.println("metadata: " + metadata);
                if (metadata != null) {
                    System.out.println(
                            "message(K:" + record.key() +
                                    ")\n\tsent to partition(" + metadata.partition() +
                                    "), topic(" + metadata.topic() +
                                    "), offset(" + metadata.offset() +
                                    "), timestamp(" + (to1 = metadata.timestamp()) + " [" + new Date(to1) + "]" +
                                    ") in " + elapsedTime/1e6 + " ms");

                    System.out.println("\nTrying to read written message back");
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        producer.flush();
        producer.close();

        System.out.println("DateTime before send():    [" + Instant.ofEpochMilli(from2) + "]");
        System.out.println("Send callback Timestamp:   [" + Instant.ofEpochMilli(to1) + "]");
        System.out.println("Elapsed to send: " + (to1 - from2) + " ms");
    }
}
