package com.pipeline;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Duration;
import java.time.Instant;

public class FeatureEngineeringJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ObjectMapper mapper = new ObjectMapper();

        String brokers = System.getenv().getOrDefault("KAFKA_BROKER", "kafka:29092");

        // 1. Define Kafka Source for user-events
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("user-events")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Parse JSON and Assign Watermarks (30-second bounded out-of-orderness)
        DataStream<JsonNode> eventStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((eventStr, timestamp) -> {
                            try {
                                JsonNode node = mapper.readTree(eventStr);
                                String timeStr = node.get("timestamp").asText();
                                return Instant.parse(timeStr).toEpochMilli();
                            } catch (Exception e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Kafka Source"
        ).map(mapper::readTree);

        // 3. User Features (1-Hour Tumbling Window)
        // Computes click_rate and avg_dwell_time
        /* eventStream.keyBy(json -> json.get("user_id").asText())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new UserFeatureAggregator()) 
                .sinkTo(featureSink);
        */

        // 4. Content Features (15-Min Sliding Window, 5-Min Slide)
        // Computes engagement_rate
        /*
        eventStream.keyBy(json -> json.get("content_id").asText())
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .aggregate(new ContentFeatureAggregator())
                .sinkTo(featureSink);
        */

        // 5. Stream-Table Join (Enrichment)
        // Computes category_affinity_score
        // (You would set up a StreamTableEnvironment here, define the content-metadata topic as a Table, 
        // and execute a temporal join or lookup join using Flink SQL).

        env.execute("Real-Time Feature Engineering Job");
    }
}