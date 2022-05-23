package nl.jtim.spring.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;

import static nl.jtim.spring.kafka.consumer.StockQuoteConsumer.STOCK_QUOTES_KAFKA_LISTENER_ID;
import static nl.jtim.spring.kafka.consumer.StockQuoteConsumer.STOCK_QUOTES_TOPIC_NAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;

@Slf4j
@Testcontainers(disabledWithoutDocker = true)
@DirtiesContext
@SpringBootTest
@Import({KafkaTestTopicTestConfiguration.class, KafkaMockSchemaRegistryTestConfiguration.class})
@ExtendWith(OutputCaptureExtension.class)
public class KafkaTestContainersIntegrationTest {

    @Container
    private static final KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.1.0"))
        .withReuse(true);

    @Autowired
    private StockQuoteService stockQuoteService;

    @Autowired
    private KafkaTemplate<String, StockQuote> kafkaTemplate;

    @Autowired
    private KafkaListenerEndpointRegistry registry;

    @DynamicPropertySource
    static void setDatasourceProperties(DynamicPropertyRegistry propertyRegistry) {
        propertyRegistry.add("spring.kafka.properties.bootstrap.servers", kafka::getBootstrapServers);
        propertyRegistry.add("spring.kafka.producer.properties.bootstrap.servers", kafka::getBootstrapServers);
    }

    @Test
    void consumerAvroTest() {
        // Get access to the listenerContainer 'consumer'
        MessageListenerContainer listenerContainer = registry.getListenerContainer(STOCK_QUOTES_KAFKA_LISTENER_ID);
        assertThat(listenerContainer).isNotNull();

        // Wait until all partitions are assigned to the 'consumer'
        ContainerTestUtils.waitForAssignment(listenerContainer, 3);

        // Produce message
        StockQuote stockQuote = new StockQuote("INGA", "AMS", "10.99", "EUR", "ING Stock", Instant.now());
        kafkaTemplate.send(STOCK_QUOTES_TOPIC_NAME, stockQuote.getSymbol(), stockQuote);

        // Verify the message has been consumed
        verify(stockQuoteService, timeout(1000)).handle(stockQuote);
    }

    @Test
    public void consumerAvroTestWithoutServiceMock(CapturedOutput output) throws InterruptedException {
        // Get access to the listenerContainer 'consumer'
        MessageListenerContainer listenerContainer = registry.getListenerContainer(STOCK_QUOTES_KAFKA_LISTENER_ID);
        assertThat(listenerContainer).isNotNull();

        // Wait until all partitions are assigned to the 'consumer'
        ContainerTestUtils.waitForAssignment(listenerContainer, 3);

        // Produce message
        StockQuote stockQuote = new StockQuote("INGA", "AMS", "10.99", "EUR", "ING Stock", Instant.now());
        kafkaTemplate.send(STOCK_QUOTES_TOPIC_NAME, stockQuote.getSymbol(), stockQuote);

        Thread.sleep(1000);

        assertThat(output.getOut()).contains("{\"symbol\": \"INGA\", \"exchange\": \"AMS\", \"tradeValue\": \"10.99\", \"currency\": \"EUR\", \"description\": \"ING Stock\",");
    }
}
