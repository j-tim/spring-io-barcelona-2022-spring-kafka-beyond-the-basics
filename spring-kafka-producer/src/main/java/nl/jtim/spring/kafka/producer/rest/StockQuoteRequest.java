package nl.jtim.spring.kafka.producer.rest;

import lombok.Getter;

@Getter
public class StockQuoteRequest {

    /**
     * The identifier of the stock.
     */
    private String symbol;
    /**
     * The stock exchange the stock was traded.
     */
    private String exchange;
    /**
     * The value the stock was traded for.
     */
    private String tradeValue;
    /**
     * The currency the stock was traded in.
     */
    private String currency;
    /**
     * Description about the stock.
     */
    private String description;

    public StockQuoteRequest(String symbol, String exchange, String tradeValue, String currency, String description) {
        this.symbol = symbol;
        this.exchange = exchange;
        this.tradeValue = tradeValue;
        this.currency = currency;
        this.description = description;
    }
}
