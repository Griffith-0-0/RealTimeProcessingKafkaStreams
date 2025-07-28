package org.example.exercice2.kafkaStreams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaStreamProcessor {

    @Bean
    public KStream<String, String> kStream(StreamsBuilder builder) {
        KStream<String, String> stream = builder.stream("clicks");

        stream
                .groupByKey()
                .count(Materialized.as("click-count-store"))
                .toStream()
                .to("click-counts", Produced.with(Serdes.String(), Serdes.Long()));

        return stream;
    }
}
