package nl.jtim.spring.kafka.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfiguration {

    public final static String STOCK_QUOTES_TOPIC_NAME = "stock-quotes";

    /**
     * Since version 2.6, you can omit .partitions() and/or replicas()
     * and the broker defaults will be applied to those properties.
     * The broker version must be at least 2.4.0 to support this feature.
     * <p>
     * See: https://cwiki.apache.org/confluence/display/KAFKA/KIP-464%3A+Defaults+for+AdminClient%23createTopic
     */
    @Bean
    @ConditionalOnProperty(name = "kafka.producer.enabled", havingValue = "true")
    public NewTopic stockQuotesTopic() {
        return TopicBuilder.name(STOCK_QUOTES_TOPIC_NAME)
            .build();
    }
}
