package com.taniyanandi.sentinel.stream;

import com.taniyanandi.sentinel.domain.AnomalyAlert;
import com.taniyanandi.sentinel.domain.DetectionState;
import com.taniyanandi.sentinel.domain.TelemetryEvent;
import com.taniyanandi.sentinel.service.DetectorStrategy;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.kstream.Transformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;

@Configuration
public class EventStreamProcessor {
    private final DetectorStrategy detector;
    private final String eventsTopic;
    private final String alertsTopic;

    public EventStreamProcessor(DetectorStrategy detector,
                                @Value("${sentinel.kafka.events-topic:sentinel.events}") String eventsTopic,
                                @Value("${sentinel.kafka.alerts-topic:sentinel.alerts}") String alertsTopic) {
        this.detector = detector;
        this.eventsTopic = eventsTopic;
        this.alertsTopic = alertsTopic;
    }

    @Bean
    KStream<String, TelemetryEvent> topology(StreamsBuilder builder) {
        JsonSerde<TelemetryEvent> eventSerde = new JsonSerde<>(TelemetryEvent.class);
        JsonSerde<AnomalyAlert> alertSerde = new JsonSerde<>(AnomalyAlert.class);
        JsonSerde<DetectionState> stateSerde = new JsonSerde<>(DetectionState.class);

        StoreBuilder<KeyValueStore<String, DetectionState>> stateStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("baseline-store"), Serdes.String(), stateSerde);
        builder.addStateStore(stateStore);

        KStream<String, TelemetryEvent> events = builder.stream(eventsTopic,
                Consumed.with(Serdes.String(), eventSerde));

        KStream<String, AnomalyAlert> alerts = events
                .selectKey((key, event) -> event.entityId() + "|" + event.metric())
                .transform(() -> new Transformer<String, TelemetryEvent, KeyValue<String, AnomalyAlert>>() {
                    private KeyValueStore<String, DetectionState> store;

                    @SuppressWarnings("unchecked")
                    @Override
                    public void init(ProcessorContext context) {
                        store = (KeyValueStore<String, DetectionState>) context.getStateStore("baseline-store");
                    }

                    @Override
                    public KeyValue<String, AnomalyAlert> transform(String key, TelemetryEvent event) {
                        DetectionState state = store.get(key);
                        if (state == null) state = new DetectionState();
                        AnomalyAlert alert = detector.evaluate(event, state);
                        store.put(key, state);
                        return alert == null ? null : KeyValue.pair(key, alert);
                    }

                    @Override
                    public void close() {}
                }, Named.as("anomaly-detector"), "baseline-store")
                .filter((key, alert) -> alert != null);

        alerts.to(alertsTopic, Produced.with(Serdes.String(), alertSerde));
        return events;
    }
}
