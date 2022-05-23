package nl.jtim.kafka.streams.config;

import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.core.KafkaOperations;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.streams.RecoveringDeserializationExceptionHandler;
import org.springframework.kafka.support.KafkaStreamBrancher;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiFunction;

import static nl.jtim.kafka.streams.config.KafkaTopicsConfiguration.STOCK_QUOTES_TOPIC_NAME;

@Configuration
@EnableKafka
@EnableKafkaStreams
public class KafkaStreamsConfig {

    @Bean
    public KStream<String, StockQuote> kStream(StreamsBuilder streamsBuilder) {

        KStream<String, StockQuote> stream = streamsBuilder.stream(STOCK_QUOTES_TOPIC_NAME);
        stream.print(Printed.toSysOut());

        KStream<String, StockQuote> branchedStream = new KafkaStreamBrancher<String, StockQuote>()
            .branch((key, value) -> value.getExchange().equalsIgnoreCase("NYSE"), kStream -> kStream.to("stock-quotes-exchange-nyse"))
            .branch((key, value) -> value.getExchange().equalsIgnoreCase("NASDAQ"), kStream -> kStream.to("stock-quotes-exchange-nasdaq"))
            .branch((key, value) -> value.getExchange().equalsIgnoreCase("AMS"), kStream -> kStream.to("stock-quotes-exchange-ams"))
            .defaultBranch(kStream -> kStream.to("stock-quotes-exchange-other"))
            .onTopOf(streamsBuilder.stream(STOCK_QUOTES_TOPIC_NAME));

        branchedStream.print(Printed.toSysOut());

        return branchedStream;
    }
}
