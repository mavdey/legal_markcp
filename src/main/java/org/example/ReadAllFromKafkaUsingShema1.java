package org.example;/* error: Exception in thread "main" org.apache.kafka.common.errors.SerializationException:
Error deserializing key/value for partition legal_markcp_nezaregi_dev-0 at offset 10504. If needed, please seek past the record to continue consumption.
Caused by: org.apache.kafka.common.errors.SerializationException: Error deserializing Avro message for id 1158
Caused by: com.google.common.util.concurrent.UncheckedExecutionException: org.apache.kafka.common.errors.SerializationException: Could not find class ru.beeline.customer.datamarts.CpForMarking
specified in writer's schema whilst finding reader's schema for a SpecificRecord.
at com.google.common.cache.LocalCache$Segment.get(LocalCache.java:2051)
at com.google.common.cache.LocalCache.get(LocalCache.java:3962)
at com.google.common.cache.LocalCache.getOrLoad(LocalCache.java:3985)
at com.google.common.cache.LocalCache$LocalLoadingCache.get(LocalCache.java:4946)
at io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer.getDatumReader(AbstractKafkaAvroDeserializer.java:231)
at io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer$DeserializationContext.read(AbstractKafkaAvroDeserializer.java:404)
at io.confluent.kafka.serializers.AbstractKafkaAvroDeserializer.deserialize(AbstractKafkaAvroDeserializer.java:152)
at io.confluent.kafka.serializers.KafkaAvroDeserializer.deserialize(KafkaAvroDeserializer.java:53)
at org.apache.kafka.common.serialization.Deserializer.deserialize(Deserializer.java:60)
at org.apache.kafka.clients.consumer.internals.Fetcher.parseRecord(Fetcher.java:1306)
at org.apache.kafka.clients.consumer.internals.Fetcher.access$3500(Fetcher.java:128)
at org.apache.kafka.clients.consumer.internals.Fetcher$CompletedFetch.fetchRecords(Fetcher.java:1537)
at org.apache.kafka.clients.consumer.internals.Fetcher$CompletedFetch.access$1700(Fetcher.java:1373)
at org.apache.kafka.clients.consumer.internals.Fetcher.fetchRecords(Fetcher.java:679)
at org.apache.kafka.clients.consumer.internals.Fetcher.fetchedRecords(Fetcher.java:634)
at org.apache.kafka.clients.consumer.KafkaConsumer.pollForFetches(KafkaConsumer.java:1313)
at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1240)
at org.apache.kafka.clients.consumer.KafkaConsumer.poll(KafkaConsumer.java:1211)
at ReadAllFromKafkaUsingShema1.main(ReadAllFromKafkaUsingShema1.java:58)
Caused by: org.apache.kafka.common.errors.SerializationException: Could not find class ru.beeline.customer.datamarts.CpForMarking specified in writer's schema whilst finding reader's schema for a
SpecificRecord. */

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.*;

public class ReadAllFromKafkaUsingShema1 {
    static long from = 0;
    static long to = 0;

    public static void main(final String[] args) throws Exception {
        final Properties props = new Properties();
        props.put("auto.offset.reset", "earliest");
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", "50"); // default is 500 (org.apache.kafka.clients.consumer.ConsumerConfig.java)
        props.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
        props.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);

        props.put("bootstrap.servers", "kfk-dev-03.dmp.vimpelcom.ru:6667");
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "GSSAPI");
        props.put("sasl.jaas.config", "com.sun.security.auth.module.Krb5LoginModule required useKeyTab=true storeKey=true useTicketCache=false debug=true keyTab=\"./tech_legalstage_ms.keytab\" principal=\"tech_legalstage_ms@BEE.VIMPELCOM.RU\" serviceName=\"kafka\";");
        props.put("schema.registry.url", "http://kfk-dev-01.dmp.vimpelcom.ru:7788/api/v1/confluent");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "console-consumer-myapp");
        String topic = "legal_markcp_nt";

        Consumer<String, GenericRecord> consumer = new KafkaConsumer<>(props);
        TopicPartition topicPartition = new TopicPartition(topic, 0);
        List<TopicPartition> partitions = Arrays.asList(topicPartition);
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);

        Iterator recordIter = Collections.emptyList().iterator();
        ConsumerRecords<String, GenericRecord> records = null;

        long time = 0;
        int cnt = 0;
        long start = System.nanoTime();

        while (!recordIter.hasNext()) {
            records = consumer.poll(Duration.ofMillis(100));
            recordIter = records.iterator();
            cnt++;
            long thisTime = System.nanoTime();
            time += (thisTime - start);
            records.forEach(record -> System.out.println(
                    "message(K:" + record.key() + ", V:" + record.value() +
                            ")\nread from partition(" + record.partition() +
                            "), topic(" + record.topic() +
                            "), offset(" + record.offset() +
                            "), timestamp(" + (to = record.timestamp()) + " [" + new Date(to) + "]" +
                            "), timestampType(" + record.timestampType() +
                            "), leaderEpoch(" + record.leaderEpoch() +
                            "), serializedKeySize(" + record.serializedKeySize() +
                            "), serializedValueSize(" + record.serializedValueSize() +
                            "), headers(" + Arrays.toString(record.headers().toArray()) + ")"
            ));
            start = thisTime;
        }
        consumer.commitAsync();
        long finish = System.nanoTime();
        long recCnt = records.count();
        System.out.println("Finished reading " + recCnt + " message(s) in " + (finish - start) / 1e6 + " ms");

        System.out.println("Looped " + cnt + " times until first messages came");
        System.out.println("records read: " + recCnt + "; elapsed " + time / 1e6 + " ms, " + time / 1.0 / recCnt + " ns/msg, " + recCnt * 1e9 / time + " msg/sec");
    }
}
