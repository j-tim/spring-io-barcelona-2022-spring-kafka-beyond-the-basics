package nl.jtim.spring.kafka.consumer;

import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;
import org.springframework.stereotype.Service;

@Service
public class ShakyStockQuoteService implements StockQuoteService {

    @Override
    public void handle(StockQuote stockQuote) {
        if ("KABOOM".equalsIgnoreCase(stockQuote.getSymbol())) {
            throw new RuntimeException("Whoops something went wrong...");
        }
    }
}

