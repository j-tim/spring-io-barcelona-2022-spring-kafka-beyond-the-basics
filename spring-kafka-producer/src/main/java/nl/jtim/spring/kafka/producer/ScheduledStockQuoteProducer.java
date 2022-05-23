package nl.jtim.spring.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import nl.jtim.spring.kafka.producer.generator.RandomStockQuoteGenerator;
import org.springframework.scheduling.annotation.Scheduled;

@Slf4j
public class ScheduledStockQuoteProducer {

    private final StockQuoteProducer producer;
    private final RandomStockQuoteGenerator generator;

    public ScheduledStockQuoteProducer(StockQuoteProducer producer, RandomStockQuoteGenerator generator) {
        this.producer = producer;
        this.generator = generator;
    }

    @Scheduled(fixedRateString = "${kafka.producer.rate}")
    public void produce() {
        StockQuote stockQuote = generator.generate();
        producer.produce(stockQuote);
    }
}
