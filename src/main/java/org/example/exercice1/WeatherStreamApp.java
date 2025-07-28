package org.example.exercice1;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WeatherStreamApp {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-streams");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> weatherStream = builder.stream("weather-data");

        KStream<String, String> filteredStream = weatherStream
                .mapValues(value -> value.trim())
                .filter((key, value) -> {
                    String[] parts = value.split(",");
                    return parts.length == 3 && Double.parseDouble(parts[1]) > 30;
                });

        KStream<String, String> fahrenheitStream = filteredStream.mapValues(value -> {
            String[] parts = value.split(",");
            String station = parts[0];
            double celsius = Double.parseDouble(parts[1]);
            double fahrenheit = (celsius * 9 / 5) + 32;
            String humidity = parts[2];
            return station + "," + fahrenheit + "," + humidity;
        });

        KGroupedStream<String, String> groupedStream = fahrenheitStream
                .groupBy((key, value) -> value.split(",")[0], Grouped.with(Serdes.String(), Serdes.String()));

        KTable<String, String> averageTable = groupedStream.aggregate(
                () -> new double[]{0.0, 0.0, 0.0}, // sumTemp, sumHumidity, count
                (key, value, agg) -> {
                    String[] parts = value.split(",");
                    double temp = Double.parseDouble(parts[1]);
                    double hum = Double.parseDouble(parts[2]);
                    agg[0] += temp;
                    agg[1] += hum;
                    agg[2] += 1;
                    return agg;
                },
                Materialized.with(Serdes.String(), Serdes.serdeFrom(
                        (topic, data) -> Arrays.toString(data).getBytes(),
                        (topic, bytes) -> {
                            String s = new String(bytes).replaceAll("[\\[\\] ]", "");
                            String[] vals = s.split(",");
                            return new double[]{
                                    Double.parseDouble(vals[0]),
                                    Double.parseDouble(vals[1]),
                                    Double.parseDouble(vals[2])
                            };
                        }
                ))
        ).mapValues(agg -> {
            double avgTemp = agg[0] / agg[2];
            double avgHum = agg[1] / agg[2];
            return String.format("Température Moyenne = %.2f°F, Humidité Moyenne = %.1f%%", avgTemp, avgHum);
        });

        // afficher dans le terminal
        filteredStream.peek((k, v) -> System.out.println("[Filtered >30°C] " + v));
        fahrenheitStream.peek((k, v) -> System.out.println("[Fahrenheit] " + v));
        averageTable.toStream()
                .peek((k, v) -> System.out.println("[Aggregated] " + k + " -> " + v))
                .to("station-averages", Produced.with(Serdes.String(), Serdes.String()));


        averageTable.toStream().to("station-averages", Produced.with(Serdes.String(), Serdes.String()));
        filteredStream.to("filtered-data", Produced.with(Serdes.String(), Serdes.String()));
        fahrenheitStream.to("fahrenheit-data", Produced.with(Serdes.String(), Serdes.String()));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        streams.start();
    }
}

/*
producer:
./kafka-console-producer.sh --topic weather-data --bootstrap-server localhost:9092
consumers:
./kafka-console-consumer.sh --topic filtered-data --bootstrap-server localhost:9092 --from-beginning
./kafka-console-consumer.sh --topic fahrenheit-data --bootstrap-server localhost:9092 --from-beginning
./kafka-console-consumer.sh --topic station-averages --bootstrap-server localhost:9092 --from-beginning --property print.key=true --property key.separator=" => "

*/

