package utils;


import org.apache.log4j.Logger;

public class Timer {

    private final Logger logger;
    private long start;

    public Timer(Logger logger) {
        this.logger = logger;
        start();
    }

    public void start() {
        start = System.nanoTime();
    }

    public void end(String text) {
        double secsDiff = (System.nanoTime() - start) / 1E9;
        logger.info(text + " " + secsDiff + " segundos.");
        start();
    }

}
