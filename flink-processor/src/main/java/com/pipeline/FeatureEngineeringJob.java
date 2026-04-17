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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.DataTypes;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

public class FeatureEngineeringJob {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        ObjectMapper mapper = new ObjectMapper();

        String brokers = System.getenv().getOrDefault("KAFKA_BROKER", "kafka:29092");

        // 1. Setup Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(brokers)
                .setTopics("user-events")
                .setGroupId("flink-feature-group")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 2. Setup Kafka Sink
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("feature-store")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();

        // 3. Ingest Data and Apply Watermarks
        DataStream<JsonNode> eventStream = env.fromSource(
                source,
                WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(30))
                        .withTimestampAssigner((eventStr, timestamp) -> {
                            try {
                                JsonNode node = mapper.readTree(eventStr);
                                return Instant.parse(node.get("timestamp").asText()).toEpochMilli();
                            } catch (Exception e) {
                                return System.currentTimeMillis();
                            }
                        }),
                "Kafka Source").map(mapper::readTree);

        // 4. User Features (Tumbling Window)
        eventStream
                .keyBy(json -> json.get("user_id").asText())
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                .aggregate(new UserFeatureAggregator())
                .flatMap(new FlatMapFunction<List<String>, String>() {
                    @Override
                    public void flatMap(List<String> values, Collector<String> out) {
                        for (String msg : values) {
                            out.collect(msg);
                        }
                    }
                })
                .sinkTo(sink);

        // 5. Content Features (Sliding Window)
        eventStream
                .keyBy(json -> json.get("content_id").asText())
                .window(SlidingEventTimeWindows.of(Time.minutes(15), Time.minutes(5)))
                .aggregate(new ContentFeatureAggregator())
                .sinkTo(sink);

        // ==========================================
        // 6. Stream-Table Join: Category Affinity
        // ==========================================

        // Convert JSON to strongly typed Row for Flink SQL
        DataStream<Row> typedEventStream = eventStream.map(json -> Row.of(
                json.get("user_id").asText(),
                json.get("content_id").asText()))
                .returns(Types.ROW_NAMED(new String[] { "user_id", "content_id" }, Types.STRING, Types.STRING));

        // Define Schema with PROCTIME
        Schema eventSchema = Schema.newBuilder()
                .column("user_id", DataTypes.STRING())
                .column("content_id", DataTypes.STRING())
                .columnByExpression("proctime", "PROCTIME()")
                .build();

        tableEnv.createTemporaryView("user_events", typedEventStream, eventSchema);

        // Define Content Metadata Table (REMOVED PRIMARY KEY)
        tableEnv.executeSql(
                "CREATE TABLE content_metadata (" +
                        "  content_id STRING," +
                        "  category STRING" +
                        ") WITH (" +
                        "  'connector' = 'kafka'," +
                        "  'topic' = 'content-metadata'," +
                        "  'properties.bootstrap.servers' = '" + brokers + "'," +
                        "  'properties.group.id' = 'metadata-enrichment-group'," +
                        "  'format' = 'json'," +
                        "  'scan.startup.mode' = 'earliest-offset'" +
                        ")");

        // Execute Join and Aggregate
        Table affinityTable = tableEnv.sqlQuery(
                "SELECT " +
                        "  u.user_id AS entity_id, " +
                        "  'affinity_' || m.category AS feature_name, " +
                        "  CAST(COUNT(*) AS STRING) AS feature_value, " +
                        "  CAST(CURRENT_TIMESTAMP AS STRING) AS computed_at " +
                        "FROM user_events u " +
                        "JOIN content_metadata m " +
                        "ON u.content_id = m.content_id " +
                        "GROUP BY u.user_id, m.category");

        // Safely extract INSERT and UPDATE_AFTER records from the Changelog Stream
        tableEnv.toChangelogStream(affinityTable)
                .filter(row -> {
                    org.apache.flink.types.RowKind kind = row.getKind();
                    return kind == org.apache.flink.types.RowKind.INSERT
                            || kind == org.apache.flink.types.RowKind.UPDATE_AFTER;
                })
                .map(row -> {
                    ObjectNode json = mapper.createObjectNode();
                    json.put("entity_id", (String) row.getField("entity_id"));
                    json.put("feature_name", (String) row.getField("feature_name"));
                    json.put("feature_value", (String) row.getField("feature_value"));
                    json.put("computed_at", (String) row.getField("computed_at"));
                    return json.toString();
                })
                .sinkTo(sink);

        env.execute("Real-Time Feature Engineering Pipeline");
    }

    // --- Aggregators ---
    public static class UserFeatureAcc {
        public String userId = "";
        public long totalEvents = 0;
        public long clicks = 0;
        public long totalDwellTimeMs = 0;
    }

    public static class UserFeatureAggregator implements AggregateFunction<JsonNode, UserFeatureAcc, List<String>> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public UserFeatureAcc createAccumulator() {
            return new UserFeatureAcc();
        }

        @Override
        public UserFeatureAcc add(JsonNode value, UserFeatureAcc acc) {
            acc.userId = value.get("user_id").asText();
            acc.totalEvents++;
            if ("click".equals(value.get("event_type").asText())) {
                acc.clicks++;
            }
            if (value.has("dwell_time_ms")) {
                acc.totalDwellTimeMs += value.get("dwell_time_ms").asLong();
            }
            return acc;
        }

        @Override
        public List<String> getResult(UserFeatureAcc acc) {
            List<String> results = new ArrayList<>();
            String now = Instant.now().toString();

            ObjectNode clickRateNode = mapper.createObjectNode();
            clickRateNode.put("entity_id", acc.userId);
            clickRateNode.put("feature_name", "click_rate");
            clickRateNode.put("feature_value",
                    String.format("%.2f", acc.totalEvents > 0 ? (double) acc.clicks / acc.totalEvents : 0.0));
            clickRateNode.put("computed_at", now);
            results.add(clickRateNode.toString());

            ObjectNode dwellTimeNode = mapper.createObjectNode();
            dwellTimeNode.put("entity_id", acc.userId);
            dwellTimeNode.put("feature_name", "avg_dwell_time");
            dwellTimeNode.put("feature_value",
                    String.format("%.2f", acc.totalEvents > 0 ? (double) acc.totalDwellTimeMs / acc.totalEvents : 0.0));
            dwellTimeNode.put("computed_at", now);
            results.add(dwellTimeNode.toString());

            return results;
        }

        @Override
        public UserFeatureAcc merge(UserFeatureAcc a, UserFeatureAcc b) {
            a.totalEvents += b.totalEvents;
            a.clicks += b.clicks;
            a.totalDwellTimeMs += b.totalDwellTimeMs;
            return a;
        }
    }

    public static class ContentFeatureAcc {
        public String contentId = "";
        public long views = 0;
        public long engagements = 0;
    }

    public static class ContentFeatureAggregator implements AggregateFunction<JsonNode, ContentFeatureAcc, String> {
        private static final ObjectMapper mapper = new ObjectMapper();

        @Override
        public ContentFeatureAcc createAccumulator() {
            return new ContentFeatureAcc();
        }

        @Override
        public ContentFeatureAcc add(JsonNode value, ContentFeatureAcc acc) {
            acc.contentId = value.get("content_id").asText();
            String eventType = value.get("event_type").asText();
            if ("view".equals(eventType)) {
                acc.views++;
            } else if ("like".equals(eventType) || "share".equals(eventType)) {
                acc.engagements++;
            }
            return acc;
        }

        @Override
        public String getResult(ContentFeatureAcc acc) {
            ObjectNode result = mapper.createObjectNode();
            result.put("entity_id", acc.contentId);
            result.put("feature_name", "engagement_rate");
            result.put("feature_value",
                    String.format("%.2f", acc.views > 0 ? (double) acc.engagements / acc.views : 0.0));
            result.put("computed_at", Instant.now().toString());
            return result.toString();
        }

        @Override
        public ContentFeatureAcc merge(ContentFeatureAcc a, ContentFeatureAcc b) {
            a.views += b.views;
            a.engagements += b.engagements;
            return a;
        }
    }
}