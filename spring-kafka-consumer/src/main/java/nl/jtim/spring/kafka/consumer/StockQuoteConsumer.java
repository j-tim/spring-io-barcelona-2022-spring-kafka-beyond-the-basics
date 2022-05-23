package nl.jtim.spring.kafka.consumer;

import lombok.extern.slf4j.Slf4j;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import javax.validation.Valid;

@Component
@Slf4j
public class StockQuoteConsumer {

    public final static String STOCK_QUOTES_TOPIC_NAME = "stock-quotes";
    public final static String STOCK_QUOTES_KAFKA_LISTENER_ID = "stockQuoteListener";

    private final StockQuoteService service;

    public StockQuoteConsumer(StockQuoteService service) {
        this.service = service;
    }

    @KafkaListener(topics = STOCK_QUOTES_TOPIC_NAME, id = STOCK_QUOTES_KAFKA_LISTENER_ID, idIsGroup = false)
    public void on(StockQuote stockQuote, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) String partition) {
        log.info("Consumed from partition: {} value: {}", partition, stockQuote);

        service.handle(stockQuote);
    }
}
