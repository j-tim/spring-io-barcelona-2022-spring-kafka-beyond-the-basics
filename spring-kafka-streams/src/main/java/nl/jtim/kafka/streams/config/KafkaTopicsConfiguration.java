package nl.jtim.kafka.streams.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicsConfiguration {

    public final static String STOCK_QUOTES_TOPIC_NAME = "stock-quotes";
    public final static String STOCK_QUOTES_EXCHANGE_NYSE_TOPIC_NAME = "stock-quotes-exchange-nyse";
    public final static String STOCK_QUOTES_EXCHANGE_NASDAQ_TOPIC_NAME = "stock-quotes-exchange-nasdaq";
    public final static String STOCK_QUOTES_EXCHANGE_AMS_TOPIC_NAME = "stock-quotes-exchange-ams";
    public final static String STOCK_QUOTES_EXCHANGE_OTHER_TOPIC_NAME = "stock-quotes-exchange-other";

    @Bean
    public NewTopic stockQuotes() {
        return TopicBuilder.name(STOCK_QUOTES_TOPIC_NAME)
            .build();
    }

    @Bean
    public NewTopic stockQuotesNyse() {
        return TopicBuilder.name(STOCK_QUOTES_EXCHANGE_NYSE_TOPIC_NAME)
            .build();
    }

    @Bean
    public NewTopic stockQuotesNasdaq() {
        return TopicBuilder.name(STOCK_QUOTES_EXCHANGE_NASDAQ_TOPIC_NAME)
            .build();
    }

    @Bean
    public NewTopic stockQuotesAmsterdam() {
        return TopicBuilder.name(STOCK_QUOTES_EXCHANGE_OTHER_TOPIC_NAME)
            .build();
    }


}
