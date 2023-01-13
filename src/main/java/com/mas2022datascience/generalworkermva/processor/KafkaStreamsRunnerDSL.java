package com.mas2022datascience.generalworkermva.processor;

import com.mas2022datascience.avro.v1.PlayerBall;
import com.mas2022datascience.avro.v1.Acceleration;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.SessionStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Map;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

@Configuration
@EnableKafkaStreams
public class KafkaStreamsRunnerDSL {

  @Value(value = "${spring.kafka.properties.schema.registry.url}") private String schemaRegistry;
  @Value(value = "${topic.general-player-ball.name}") private String topicIn;
  @Value(value = "${topic.general-acceleration.name}") private String topicOut;

  // Windowing
  @Value(value = "${acceleration.parameter.session-length}") private long sessionLength;
  @Value(value = "${acceleration.parameter.session-grace-time}") private long sessionGraceTime;
  @Value(value = "${acceleration.parameter.velocity-threshold}") private long velocityThreshold;
  @Value(value = "${acceleration.parameter.acceleration-threshold}") private long accelerationThreshold;
  @Value(value = "${acceleration.parameter.min-acceleration-length}") private long minSprintLength;

  // MVA
  @Value(value = "${mva.slope}") private double slope;
  @Value(value = "${mva.intercept}") private double intercept;
  @Value(value = "${mva.vipd}") private double vipd;

  @Value(value = "${mva.vipd-percent-threshold}") private double vipdPercentThreshold;
  @Value(value = "${mva.mva-percent-threshold}") private double mvaPercentThreshold;

  @Bean
  public KStream<String, PlayerBall> kStream(StreamsBuilder kStreamBuilder) {

    // When you want to override serdes explicitly/selectively
    final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url",
        schemaRegistry);

    final Serde<PlayerBall> playerBallSerde = new SpecificAvroSerde<>();
    playerBallSerde.configure(serdeConfig, false); // `false` for record values

    final Serde<Acceleration> accelerationSerde = new SpecificAvroSerde<>();
    accelerationSerde.configure(serdeConfig, false); // `false` for record values

    final StoreBuilder<KeyValueStore<String, PlayerBall>> sprintStore = Stores
        .keyValueStoreBuilder(Stores.persistentKeyValueStore("AccelerationStore"),
            Serdes.String(), playerBallSerde);
    kStreamBuilder.addStateStore(sprintStore);

    SessionWindows sessionWindow = SessionWindows.ofInactivityGapAndGrace(
        Duration.ofMillis(sessionLength), Duration.ofMillis(sessionGraceTime));

    KStream<String, PlayerBall> stream = kStreamBuilder.stream(topicIn,
        Consumed.with(Serdes.String(), playerBallSerde)
            .withTimestampExtractor(new PlayerBallEventTimestampExtractor()));

    KGroupedStream<String, PlayerBall> grouped = stream
        .filter((k, v) -> {
          if (v.getVelocity() == null || v.getAccelleration() == null) return false;
          return v.getVelocity() > velocityThreshold && v.getAccelleration() > accelerationThreshold;
        })
        .groupByKey(Grouped.with(Serdes.String(), playerBallSerde));

    Aggregator<String, PlayerBall, Acceleration> aggregator = (key, value, aggValue) -> {
      aggValue.setTs(value.getTs());
      aggValue.setPlayerId(value.getId());
      aggValue.setMatchId(value.getMatchId());
      aggValue.setVMin(Math.min(aggValue.getVMin(), value.getVelocity()));
      aggValue.setVMax(Math.max(aggValue.getVMax(), value.getVelocity()));
      aggValue.setAMin(Math.min(aggValue.getAMin(), value.getAccelleration()));
      aggValue.setAMax(Math.max(aggValue.getAMax(), value.getAccelleration()));
      aggValue.setSessionStartTs(aggValue.getSessionStartTs());
      aggValue.setSessionEndTs(aggValue.getSessionStartTs());
      aggValue.setTickCount(aggValue.getTickCount()+1);
      aggValue.setSessionLengthMs(aggValue.getSessionLengthMs());
      return aggValue;
    };

    Merger<String, Acceleration> merger = (key, value1, value2) -> Acceleration.newBuilder()
        .setTs(value1.getTs().isBefore(value2.getTs()) ? value1.getTs() : value2.getTs())
        .setPlayerId(value1.getPlayerId())
        .setMatchId(value1.getMatchId())
        .setVMax(Math.max(value1.getVMax(), value2.getVMax()))
        .setVMin(Math.min(value1.getVMin(), value2.getVMin()))
        .setAMax(Math.max(value1.getAMax(), value2.getAMax()))
        .setAMin(Math.min(value1.getAMin(), value2.getAMin()))
        .setSessionStartTs(value1.getSessionStartTs())
        .setSessionEndTs(value1.getSessionEndTs())
        .setTickCount(value2.getTickCount())
        .setSessionLengthMs(value1.getSessionLengthMs())
        .build();

    KTable<Windowed<String>, Acceleration> sumOfValues = grouped
        .windowedBy(sessionWindow)
        .aggregate(
            // initializer
            () -> Acceleration.newBuilder()
                .setTs(Instant.ofEpochMilli(Long.MAX_VALUE))
                .setPlayerId("")
                .setMatchId("")
                .setVMin(Long.MAX_VALUE)
                .setVMax(Long.MIN_VALUE)
                .setAMin(Long.MAX_VALUE)
                .setAMax(Long.MIN_VALUE)
                .setSessionStartTs(Instant.ofEpochMilli(Long.MAX_VALUE).toString())
                .setSessionEndTs(Instant.ofEpochMilli(Long.MIN_VALUE).toString())
                .setTickCount(0)
                .setSessionLengthMs(0)
                .build(),
            // aggregator
            aggregator,
            // session merger
            merger,
            // serializer
            Materialized.<String, Acceleration, SessionStore<Bytes, byte[]>>as("acceleration-store")
                .withKeySerde(Serdes.String())
                .withValueSerde(accelerationSerde)
        )
        .suppress(untilWindowCloses(unbounded())) // suppress until window closes
        .mapValues((k, v) -> {
          v.setSessionStartTs(Instant.ofEpochMilli(k.window().start()).toString());
          v.setSessionEndTs(Instant.ofEpochMilli(k.window().end()).toString());
          v.setSessionLengthMs(k.window().end() - k.window().start());
          return v;
        })
        .filter((k, v) -> (k.window().end() - k.window().start() > minSprintLength));

    // publish result
    sumOfValues
        .toStream()
        .filter((k, v) -> !(v.getPlayerId().equals("0"))) // filter out the ball
        .selectKey((key, value) -> key.key()) // remove window from key
        .mapValues(v -> {
          double mva = slope * v.getVMin() + intercept;
          double vipdPercent = 100 / vipd * v.getVMin();
          v.setMvaPercent(100 / mva * v.getAMax());
          // sprint classification
          if (v.getMvaPercent() < mvaPercentThreshold) {
            if (vipdPercent < vipdPercentThreshold) {
              // Joggen / JOG
              v.setType(AccelerationTypes.JOG.getAbbreviation());
            } else {
              // Steigerungslauf / INCREMENTALRUN
              v.setType(AccelerationTypes.INCREMENTALRUN.getAbbreviation());
            }
          } else {
            if (vipdPercent < vipdPercentThreshold) {
              // Kurze Beschleunigung / SHORTACCELERATION
              v.setType(AccelerationTypes.SHORTACCELERATION.getAbbreviation());
            } else {
              // Sprint / SPRINT
              v.setType(AccelerationTypes.SPRINT.getAbbreviation());
            }
          }
          return v;
        })
        .to(topicOut);

    return stream;

  }
}

