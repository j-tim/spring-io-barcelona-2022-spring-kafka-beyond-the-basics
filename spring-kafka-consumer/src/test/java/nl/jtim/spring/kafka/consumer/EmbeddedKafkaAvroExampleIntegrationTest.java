package nl.jtim.spring.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;

import java.time.Instant;

import static nl.jtim.spring.kafka.consumer.StockQuoteConsumer.STOCK_QUOTES_KAFKA_LISTENER_ID;
import static nl.jtim.spring.kafka.consumer.StockQuoteConsumer.STOCK_QUOTES_TOPIC_NAME;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

/**
 * Example integration test with Avro producer and consumer.
 * We mock out the schema registry using {@link io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient}.
 */
@Slf4j
@EmbeddedKafka
@SpringBootTest
@Import(KafkaMockSchemaRegistryTestConfiguration.class)
public class EmbeddedKafkaAvroExampleIntegrationTest {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @Autowired
    private StockQuoteService stockQuoteService;

    @Autowired
    private KafkaTemplate<String, StockQuote> kafkaTemplate;

    @Test
    public void consumerAvroTest() {
        String kafkaBrokers = embeddedKafkaBroker.getBrokersAsString();
        log.info("Kafka Brokers: {}", kafkaBrokers);

        // Get access to the listenerContainer 'consumer'
        MessageListenerContainer listenerContainer = registry.getListenerContainer(STOCK_QUOTES_KAFKA_LISTENER_ID);

        // Wait until all partitions are assigned to the 'consumer'
        ContainerTestUtils.waitForAssignment(listenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());

        // Produce message
        StockQuote stockQuote = new StockQuote("INGA", "AMS", "10.99", "EUR", "Description", Instant.now());
        kafkaTemplate.send(STOCK_QUOTES_TOPIC_NAME, stockQuote.getSymbol(), stockQuote);

        // Verify the message has been consumed
        verify(stockQuoteService, timeout(1000)).handle(stockQuote);
    }
}