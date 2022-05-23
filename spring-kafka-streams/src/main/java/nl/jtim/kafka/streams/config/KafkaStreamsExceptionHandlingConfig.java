package nl.jtim.kafka.streams.config;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

@Configuration
public class KafkaStreamsExceptionHandlingConfig {

    private static final BiFunction<ConsumerRecord<?, ?>, Exception, TopicPartition>
        CUSTOM_DESTINATION_RESOLVER = (cr, e) -> new TopicPartition(cr.topic() + ".DEAD_LETTER_TOPIC", cr.partition());

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs(KafkaProperties kafkaProperties, DeadLetterPublishingRecoverer deadLetterPublishingRecoverer) {
        Map<String, Object> properties = kafkaProperties.getStreams().buildProperties();

        properties.put(RecoveringDeserializationExceptionHandler.KSTREAM_DESERIALIZATION_RECOVERER, deadLetterPublishingRecoverer);

        return new KafkaStreamsConfiguration(properties);
    }

    @Bean
    public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<?, ?> bytesTemplate) {
        Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesTemplate);

        // In case you want to customize the destination resolver
        return new DeadLetterPublishingRecoverer(templates, CUSTOM_DESTINATION_RESOLVER);
    }
}
