package nl.jtim.spring.kafka.consumer;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.TopicBuilder;

import static nl.jtim.spring.kafka.consumer.StockQuoteConsumer.STOCK_QUOTES_TOPIC_NAME;

/**
 * Test configuration to create topic in Embedded Kafka integration test.
 */
@TestConfiguration
public class KafkaTestTopicTestConfiguration {

    @Bean
    public NewTopic stockQuoteTestTopic() {
        return TopicBuilder.name(STOCK_QUOTES_TOPIC_NAME)
            .partitions(3)
            .replicas(1)
            .build();
    }
}
