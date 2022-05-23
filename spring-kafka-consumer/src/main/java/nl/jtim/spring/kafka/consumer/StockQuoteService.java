package nl.jtim.spring.kafka.consumer;

import nl.jtim.spring.kafka.avro.stock.quote.StockQuote;

public interface StockQuoteService {


    void handle(StockQuote stockQuote);
}
