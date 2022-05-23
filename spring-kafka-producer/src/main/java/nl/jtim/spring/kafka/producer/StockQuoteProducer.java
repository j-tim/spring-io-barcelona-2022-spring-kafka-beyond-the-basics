package nl.jtim.spring.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class StockQuoteProducer {

    private final KafkaTemplate<String, StockQuote> kafkaTemplate;

    public StockQuoteProducer(KafkaTemplate<String, StockQuote> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void produce(StockQuote stockQuote) {
        kafkaTemplate.send("stock-quotes", stockQuote.getSymbol(), stockQuote);
        log.info("Produced stock quote: {}", stockQuote);
    }
}
