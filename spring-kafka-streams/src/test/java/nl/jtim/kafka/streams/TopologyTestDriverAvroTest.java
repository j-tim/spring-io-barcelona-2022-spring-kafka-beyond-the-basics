package nl.jtim.kafka.streams;

import io.confluent.kafka.schemaregistry.testutil.MockSchemaRegistry;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import nl.jtim.kafka.streams.config.KafkaStreamsConfig;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Map;
import java.util.Properties;

import static nl.jtim.kafka.streams.config.KafkaTopicsConfiguration.*;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for Kafka streams application using the
 * Kafka streams {@link TopologyTestDriver}
 */
public class TopologyTestDriverAvroTest {

    private static final String SCHEMA_REGISTRY_SCOPE = TopologyTestDriverAvroTest.class.getName();

    private static final String MOCK_SCHEMA_REGISTRY_URL = "mock://" + SCHEMA_REGISTRY_SCOPE;
    private TopologyTestDriver topologyTestDriver;

    private TestInputTopic<String, StockQuote> stockQuoteTopic;
    private TestOutputTopic<String, StockQuote> stockQuoteNyseTopic;
    private TestOutputTopic<String, StockQuote> stockQuoteNasdaqTopic;
    private TestOutputTopic<String, StockQuote> stockQuoteAmsTopic;
    private TestOutputTopic<String, StockQuote> stockQuoteOtherTopic;

    @BeforeEach
    void setUp() {
        StreamsBuilder builder = new StreamsBuilder();
        new KafkaStreamsConfig().kStream(builder);
        Topology topology = builder.build();

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit-test");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:9092");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
        properties.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);

        topologyTestDriver = new TopologyTestDriver(topology, properties);

        Serde<String> stringSerde = Serdes.String();
        Serde<StockQuote> avroStockQuoteSerde = new SpecificAvroSerde<>();

        Map<String, String> schemaRegistryProperties = Map.of(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, MOCK_SCHEMA_REGISTRY_URL);
        avroStockQuoteSerde.configure(schemaRegistryProperties, false);

        // Define input and output topics to use in tests
        stockQuoteTopic = topologyTestDriver.createInputTopic(
                STOCK_QUOTES_TOPIC_NAME,
                stringSerde.serializer(),
                avroStockQuoteSerde.serializer());

        stockQuoteNyseTopic = topologyTestDriver.createOutputTopic(
                STOCK_QUOTES_EXCHANGE_NYSE_TOPIC_NAME,
                stringSerde.deserializer(),
                avroStockQuoteSerde.deserializer());

        stockQuoteNasdaqTopic = topologyTestDriver.createOutputTopic(
                STOCK_QUOTES_EXCHANGE_NASDAQ_TOPIC_NAME,
                stringSerde.deserializer(),
                avroStockQuoteSerde.deserializer());

        stockQuoteAmsTopic = topologyTestDriver.createOutputTopic(
                STOCK_QUOTES_EXCHANGE_AMS_TOPIC_NAME,
                stringSerde.deserializer(),
                avroStockQuoteSerde.deserializer());

        stockQuoteOtherTopic = topologyTestDriver.createOutputTopic(
                STOCK_QUOTES_EXCHANGE_OTHER_TOPIC_NAME,
                stringSerde.deserializer(),
                avroStockQuoteSerde.deserializer());
    }

    @AfterEach
    void tearDown() {
        topologyTestDriver.close();
        MockSchemaRegistry.dropScope(SCHEMA_REGISTRY_SCOPE);
    }

    @Test
    void stockQuoteFromAmsterdamStockExchangeEndUpOnTopicQuotesAmsTopic() {
        StockQuote stockQuote = new StockQuote("INGA", "AMS", "10.99", "EUR", "Description", Instant.now());

        stockQuoteTopic.pipeInput(stockQuote.getSymbol(), stockQuote);

        assertThat(stockQuoteAmsTopic.isEmpty()).isFalse();
        assertThat(stockQuoteAmsTopic.getQueueSize()).isEqualTo(1L);
        assertThat(stockQuoteAmsTopic.readValue()).isEqualTo(stockQuote);

        assertThat(stockQuoteNyseTopic.isEmpty()).isTrue();
        assertThat(stockQuoteNasdaqTopic.isEmpty()).isTrue();
        assertThat(stockQuoteOtherTopic.isEmpty()).isTrue();
    }
}
