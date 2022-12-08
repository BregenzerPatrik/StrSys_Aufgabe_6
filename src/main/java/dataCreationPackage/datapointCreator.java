package dataCreationPackage;

import kafkaPackage.KafkaProducer;

import java.util.Random;

public class datapointCreator implements Runnable {
    private final int numberOfSensors;
    private final double minSpeed;
    private final double maxSpeed;
    private final int minDelay;
    private final int maxDelay;
    private final int minNumberOfValues;
    private final int maxNumberOfValues;
    private final Random r = new Random(420691337);

    public datapointCreator(int numberOfSensors, double minSpeed, double maxSpeed, int minDelay, int maxDelay, int minNumberOfValues, int maxNumberOfValues) {
        this.numberOfSensors = numberOfSensors;
        this.minSpeed = minSpeed;
        this.maxSpeed = maxSpeed;
        this.minDelay = minDelay;
        this.maxDelay = maxDelay;
        this.minNumberOfValues = minNumberOfValues;
        this.maxNumberOfValues = maxNumberOfValues;
    }

    private double createSingleValue() {
        return minSpeed + (maxSpeed - minSpeed) * r.nextDouble();
    }

    private String getSingleSensorLine(int numberOfValues) {
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < numberOfValues; ++i) {
            double newValue = createSingleValue();
            result.append(newValue);
            boolean isLastNumber = i == numberOfValues - 1;
            if (!isLastNumber) {
                result.append(",");
            }
        }
        return result.toString();
    }

    public int getRandomNumberOfValues() {
        return (int) ((r.nextDouble() * (maxNumberOfValues - minNumberOfValues)) + minNumberOfValues);
    }

    public int getRandomSensorId() {
        return (int) ((r.nextDouble() * numberOfSensors));
    }

    public int getRandomDelay() {
        return (int) ((r.nextDouble() * (maxDelay - minDelay)) + minDelay);
    }

    @Override
    public void run() {
        while (true) {
            /*System.out.println(Integer.toString(
                            getRandomSensorId())+": "+
                    getSingleSensorLine(getRandomNumberOfValues()));*/
            KafkaProducer.sendPathData(Integer.toString(
                            getRandomSensorId()),
                    getSingleSensorLine(getRandomNumberOfValues()));
            try {
                Thread.sleep(getRandomDelay());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    //Konfigurierbar:
    //  anzahl der ErzeugtenGeschwindigkeitswerte
    //  minimale Geschwindigkeit (soll auch negativ möglich sein)
    //  maximale Geschwindigkeit
    //taktung (abstand zwischen 2 erzeugten Datensätzen)
    //  soll zufällig sein innerhalb eines gegebenen fensters


}


