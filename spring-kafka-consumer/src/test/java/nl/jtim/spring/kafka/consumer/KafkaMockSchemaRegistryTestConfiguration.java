package nl.jtim.spring.kafka.consumer;

import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.mockito.Mockito.mock;

@TestConfiguration
public class KafkaMockSchemaRegistryTestConfiguration {

    private final KafkaProperties kafkaProperties;

    public KafkaMockSchemaRegistryTestConfiguration(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    /**
     * We don't want to use the default {@link CachedSchemaRegistryClient} since it will try to
     * connect to a `real` schema registry.
     * <p>
     * Spring Kafka's {@link EmbeddedKafka} doesn't support a schema registry out of the box.
     * We will use the {@link MockSchemaRegistryClient}
     */
    @Bean
    public SchemaRegistryClient schemaRegistryClient() {
        return new MockSchemaRegistryClient();
    }

    /**
     * We also need to mock our 'service' to be able to verify
     * messages are consumed in the consumer.
     */
    @Bean
    @Primary
    public StockQuoteService stockQuoteService() {
        return mock(StockQuoteService.class);
    }

    // ============ For consuming ======================================================================================

    @Bean
    public KafkaAvroDeserializer kafkaAvroDeserializer(SchemaRegistryClient schemaRegistryClient) {
        return new KafkaAvroDeserializer(schemaRegistryClient, kafkaProperties.buildConsumerProperties());
    }

    @Bean
    public DefaultKafkaConsumerFactory<String, StockQuote> consumerFactory(KafkaAvroDeserializer kafkaAvroDeserializer) {
        return new DefaultKafkaConsumerFactory(kafkaProperties.buildConsumerProperties(), new StringDeserializer(), kafkaAvroDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, StockQuote> kafkaListenerContainerFactory(DefaultKafkaConsumerFactory<String, StockQuote> defaultKafkaConsumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, StockQuote> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(defaultKafkaConsumerFactory);
        return factory;
    }

    // ============ For producing ======================================================================================

    @Bean
    public KafkaAvroSerializer kafkaAvroSerializer(SchemaRegistryClient schemaRegistryClient) {
        return new KafkaAvroSerializer(schemaRegistryClient);
    }

    @Bean
    public DefaultKafkaProducerFactory<String, StockQuote> producerFactory(KafkaAvroSerializer kafkaAvroSerializer) {
        return new DefaultKafkaProducerFactory(kafkaProperties.buildProducerProperties(), new StringSerializer(), kafkaAvroSerializer);
    }

    @Bean
    public KafkaTemplate<String, StockQuote> kafkaTemplate(DefaultKafkaProducerFactory<String, StockQuote> producerFactory) {
        return new KafkaTemplate<>(producerFactory);
    }

}
