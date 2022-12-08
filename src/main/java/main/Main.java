package main;

import beamPackage.beamConsumer;
import dataCreationPackage.datapointCreator;

public class Main {
    public static void main(String[] args) {
        System.setProperty(org.slf4j.simple.SimpleLogger.DEFAULT_LOG_LEVEL_KEY, "off");
        beamConsumer consumer = new beamConsumer();
        Thread consume = new Thread(consumer);
        consume.start();
        datapointCreator creator = new datapointCreator(7, -5.0, 50.8, 50, 200, 0, 6);

        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        Thread publish = new Thread(creator);
        publish.start();
    }
}