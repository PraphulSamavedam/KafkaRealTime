package org.sama.wikimedia;

/** This interface represents the Producer functionality to send Wikimedia Stream data including getProducerProperties, properties.
 * */
public interface Producer {

    /** This method closes the terminal for producing any results.
     * */
    void close();

    /**
     * This method asynchronously sends the data to the configured data
     * */
    void send(String data);
}
