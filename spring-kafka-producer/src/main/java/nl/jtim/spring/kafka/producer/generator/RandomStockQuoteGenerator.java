package nl.jtim.spring.kafka.producer.generator;

import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import nl.jtim.spring.kafka.producer.generator.AbstractRandomStockQuoteGenerator;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.Instant;

@Component
public class RandomStockQuoteGenerator extends AbstractRandomStockQuoteGenerator {

    public StockQuote generate() {
        Instrument randomInstrument = pickRandomInstrument();
        BigDecimal randomPrice = generateRandomPrice();
        return new StockQuote(randomInstrument.getSymbol(), randomInstrument.getExchange(), randomPrice.toPlainString(),
            randomInstrument.getCurrency(), randomInstrument.getSymbol() + " stock", Instant.now());
    }
}
