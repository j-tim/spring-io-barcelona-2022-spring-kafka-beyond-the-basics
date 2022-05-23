package nl.jtim.spring.kafka.consumer.config;

import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Bytes;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.*;
import org.springframework.kafka.support.ProducerListener;
import org.springframework.kafka.support.converter.RecordMessageConverter;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.BackOffExecution;
import org.springframework.util.backoff.ExponentialBackOff;
import org.springframework.util.backoff.FixedBackOff;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Uncomment to demo the exception handling scenarios.
 */
//@Configuration
public class KafkaExceptionHandlingConfiguration {

    private final KafkaProperties properties;


    public KafkaExceptionHandlingConfiguration(KafkaProperties properties) {
        this.properties = properties;
    }

    /**
     * This is the default error handler.
     * Spring will 'autoconfigure' this error handler for you in case you don't specify the bean.
     */
    @Bean
    public DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler();
    }

//    @Bean
//    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
//        return new DefaultErrorHandler(recoverer);
//    }

    /**
     * Configure the {@link DeadLetterPublishingRecoverer} to publish poison pill bytes to a dead letter topic:
     * "stock-quotes.DLT".
     */
    @Bean
    public DeadLetterPublishingRecoverer recoverer(KafkaTemplate<?, ?> bytesKafkaTemplate, KafkaTemplate<?, ?> kafkaTemplate) {
        Map<Class<?>, KafkaOperations<? extends Object, ? extends Object>> templates = new LinkedHashMap<>();
        templates.put(byte[].class, bytesKafkaTemplate);
        templates.put(StockQuote.class, kafkaTemplate);
        return new DeadLetterPublishingRecoverer(templates);
    }

    /**
     * In case you don't specify a {@link org.springframework.util.backoff.BackOff}
     * the {@link org.springframework.kafka.listener.SeekUtils#DEFAULT_BACK_OFF} will be configured.
     *
     * It's a {@link FixedBackOff} with 0 interval and will try 10 times.
     */
//    @Bean
//    public DefaultErrorHandler errorHandler(DeadLetterPublishingRecoverer recoverer) {
//        return new DefaultErrorHandler(recoverer, new FixedBackOff(0, 5));
//    }



    /**
     * Implementation of {@link BackOff} that increases the back off period for each
     * retry attempt. When the interval has reached the {@link ExponentialBackOff#setMaxInterval(long)
     * max interval}, it is no longer increased. Stops retrying once the
     * {@link ExponentialBackOff#setMaxElapsedTime(long) max elapsed time} has been reached.
     *
     * <p>Example: The default interval is {@value ExponentialBackOff#DEFAULT_INITIAL_INTERVAL} ms,
     * the default multiplier is {@value ExponentialBackOff#DEFAULT_MULTIPLIER}, and the default max
     * interval is {@value ExponentialBackOff#DEFAULT_MAX_INTERVAL}. For 10 attempts the sequence will be
     * as follows:
     *
     * <pre>
     * request#     back off
     *
     *  1              2000
     *  2              3000
     *  3              4500
     *  4              6750
     *  5             10125
     *  6             15187
     *  7             22780
     *  8             30000
     *  9             30000
     * 10             30000
     * </pre>
     *
     * <p>Note that the default max elapsed time is {@link Long#MAX_VALUE}. Use
     * {@link ExponentialBackOff#setMaxElapsedTime(long)} to limit the maximum length of time
     * that an instance should accumulate before returning
     * {@link BackOffExecution#STOP}.
     */
//    @Bean
//    public DefaultErrorHandler errorHandlerWithExponentialBackOff(DeadLetterPublishingRecoverer recoverer) {
//        ExponentialBackOff backOff = new ExponentialBackOff(2_000, 1.5);
//        backOff.setMaxElapsedTime(MINUTES.toMillis(2));
//        return new DefaultErrorHandler(recoverer, backOff);
//    }
//
//    @Bean
//    public DefaultErrorHandler errorHandler() {
//        ExponentialBackOffWithMaxRetries backOffWithMaxRetries = new ExponentialBackOffWithMaxRetries(10);
//        backOffWithMaxRetries.setInitialInterval(1_000);
//        backOffWithMaxRetries.setMultiplier(2.0);
//        backOffWithMaxRetries.setMaxInterval(10_000);
//
//        return new DefaultErrorHandler(backOffWithMaxRetries);
//    }
//
//    /**
//     * We can also use the {@link CommonLoggingErrorHandler} in case we only want to
//     * log the error and continue.
//     *
//     * Boot will autowire this into the container factory.
//     */
//    @Bean
//    public CommonLoggingErrorHandler loggingErrorHandler() {
//        return new CommonLoggingErrorHandler();
//    }
//
//    @Bean
//    public CommonContainerStoppingErrorHandler containerStoppingErrorHandler() {
//        return new CommonContainerStoppingErrorHandler();
//    }
//
//    /**
//     * To mix and match error handlers based on different exception
//     */
//    @Bean
//    public CommonDelegatingErrorHandler delegatingErrorHandler() {
//        DefaultErrorHandler defaultErrorHandler = new DefaultErrorHandler();
//        CommonDelegatingErrorHandler delegatingErrorHandler = new CommonDelegatingErrorHandler(defaultErrorHandler);
//        CommonContainerStoppingErrorHandler containerStoppingErrorHandler = new CommonContainerStoppingErrorHandler();
//
//        delegatingErrorHandler.addDelegate(DeserializationException.class, containerStoppingErrorHandler);
//
//        return delegatingErrorHandler;
//    }



    /**
     * This is the specific Producer for serialization exceptions.
     * We configure ByteArraySerializer for both the key and value serializer.
     * Because we don't know upfront in what 'format' the record from the topic caused the deserialization exception.
     */
    @Bean
    public ProducerFactory<Bytes, Bytes> bytesProducerFactory(KafkaProperties kafkaProperties) {
        Map<String, Object> producerProperties = kafkaProperties.buildProducerProperties();
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        return new DefaultKafkaProducerFactory<>(producerProperties);
    }

    /**
     * This is the specific Kafka template for serialization exceptions.
     */
    @Bean
    public KafkaTemplate<Bytes, Bytes> bytesKafkaTemplate(ProducerFactory<Bytes, Bytes> bytesProducerFactory) {
        return new KafkaTemplate<>(bytesProducerFactory);
    }

    /**
     * We have to also create the "default" kafkaProducerFactory.
     * The code is basically copied from:
     * {@link org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaProducerFactory(ObjectProvider)}
     */
    @Bean
    public ProducerFactory<?, ?> kafkaProducerFactory(
        ObjectProvider<DefaultKafkaProducerFactoryCustomizer> customizers) {
        DefaultKafkaProducerFactory<?, ?> factory = new DefaultKafkaProducerFactory<>(
            this.properties.buildProducerProperties());
        String transactionIdPrefix = this.properties.getProducer().getTransactionIdPrefix();
        if (transactionIdPrefix != null) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        customizers.orderedStream().forEach((customizer) -> customizer.customize(factory));
        return factory;
    }

    /**
     * We have to also create the "default" kafkaTemplate.
     * The code is basically copied from:
     * {@link org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration#kafkaTemplate(ProducerFactory, ProducerListener, ObjectProvider)}
     */
    @Bean
    public KafkaTemplate<?, ?> kafkaTemplate(ProducerFactory<Object, Object> kafkaProducerFactory,
                                             ProducerListener<Object, Object> kafkaProducerListener,
                                             ObjectProvider<RecordMessageConverter> messageConverter) {
        KafkaTemplate<Object, Object> kafkaTemplate = new KafkaTemplate<>(kafkaProducerFactory);
        messageConverter.ifUnique(kafkaTemplate::setMessageConverter);
        kafkaTemplate.setProducerListener(kafkaProducerListener);
        kafkaTemplate.setDefaultTopic(this.properties.getTemplate().getDefaultTopic());
        return kafkaTemplate;
    }
}
