package beamPackage;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.io.kafka.KafkaRecord;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.joda.time.Duration;

import java.io.Serializable;
import java.text.DecimalFormat;

public class beamConsumer implements Runnable {
    public static class AverageFn extends Combine.CombineFn<Double, AverageFn.Accum, Double> {
        public static class Accum implements Serializable {
            double sum = 0; // store the current sum
            int count = 0; // store the number of elements already processed
        }

        @Override
        public Accum createAccumulator() {
            return new Accum();
        }

        @Override
        public Accum addInput(Accum accum, Double input) {
            if (accum != null) {
                accum.sum += input;
                accum.count++;
            }
            return accum;
        }

        @Override
        public Accum mergeAccumulators(Iterable<Accum> accums) {
            Accum merged = createAccumulator();
            for (Accum accum : accums) {
                merged.sum += accum.sum;
                merged.count += accum.count;
            }
            return merged;
        }

        @Override
        public Double extractOutput(Accum accum) {
            if (accum != null) {
                return (accum.sum) / (double)accum.count;
            }
            return 0.0;
        }
    }


    static class KafkaToString extends DoFn<KafkaRecord<String, String>, KV<String, String>> {
        @ProcessElement
        public void processElement(@Element KafkaRecord<String, String> input, OutputReceiver<KV<String, String>> output) {
            // System.out.println(input.getKV().getValue());
            KV<String, String> out = input.getKV();
            output.output(out);
        }
    }

    static class PrintStream extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element KV<String, Double> input, OutputReceiver<KV<String, Double>> output) {
            DecimalFormat f = new DecimalFormat("#0.00");
            String printString="Mean for Sensor " +input.getKey() + " in the last 5 sec: " + f.format(input.getValue())+"\t";
            for(int i=0;i< input.getValue()/2;++i){
                printString+="X";
            }
            System.out.println(printString);
            output.output(input);
        }
    }
    static class ConvertToKMH extends DoFn<KV<String, Double>, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element KV<String, Double> input, OutputReceiver<KV<String, Double>> output) {
            KV<String,Double> newOutput=KV.of(input.getKey(), input.getValue() * 3.6);
            output.output(newOutput);
        }
    }


    static class PrintDoubleStream extends DoFn<Double, Double> {
        @ProcessElement
        public void processElement(@Element Double input, OutputReceiver<Double> output) {
            DecimalFormat f = new DecimalFormat("#0.00");
            System.out.println("--------------------------------------------------");
            System.out.println("Mean for all Sensors in the last 5 sec: " + f.format(input));
            System.out.println("--------------------------------------------------");
            output.output(input);
        }
    }

    static class combineSensors extends DoFn<KV<String, Double>, Double> {
        @ProcessElement
        public void processElement(@Element KV<String, Double> input, OutputReceiver<Double> output) {
            output.output(input.getValue());
        }
    }

    static class SplitStrings extends DoFn<KV<String, String>, KV<String, Double>> {
        @ProcessElement
        public void processElement(@Element KV<String, String> input, OutputReceiver<KV<String, Double>> output) {
            if (!input.getValue().equals("")) {
                for (String measurement : input.getValue().split(",")) {
                    output.output(KV.of(input.getKey(), Double.valueOf(measurement)));
                }
            }
        }
    }


    @Override
    public void run() {
        PipelineOptions options = PipelineOptionsFactory.create();
        Pipeline pipeline = Pipeline.create(options);

        //get key and value from Kafka
        PCollection<KafkaRecord<String, String>> kafkaRecords =
                pipeline
                        .apply(KafkaIO.<String, String>read()
                                .withBootstrapServers("localhost:9092")
                                .withTopic("Measurements")
                                .withKeyDeserializer(StringDeserializer.class)
                                .withValueDeserializer(StringDeserializer.class));

        PCollection<KV<String, String>> rawKeyAndValues = kafkaRecords.apply(ParDo.of(new KafkaToString()));

        //Split list string into single elements
        PCollection<KV<String, Double>> idToSingleValue = rawKeyAndValues.apply(ParDo.of(new SplitStrings()));

        //filter by value (remove negative)
        PCollection<KV<String, Double>> filteredValuesAsKV = idToSingleValue
                .apply(Filter.by((SerializableFunction<KV<String, Double>, Boolean>) speedEvent -> {
                    return speedEvent.getValue() >= 0; //speedEvent.getValue() >= 0;
                }));
        PCollection<KV<String, Double>> idToKMH = filteredValuesAsKV.apply(ParDo.of(new ConvertToKMH()));


        //get average for individual sensors
        //  window data
        PCollection<KV<String, Double>> allSensorDataWindowed = idToKMH
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
        //  get Mean Value of Sensors every x seconds
        PCollection<KV<String, Double>> groupedSpeed =
                allSensorDataWindowed.apply(Mean.perKey());
        //  print current Stream
        groupedSpeed.apply(ParDo.of(new PrintStream()));




        //get average of all sensors
        PCollection<Double> allSensorsAsOne = idToKMH.apply(ParDo.of(new combineSensors()));

        PCollection<Double> windowedForGlobalAverage = allSensorsAsOne
                .apply(Window.into(FixedWindows.of(Duration.standardSeconds(5))));
        //  get Mean Value of Sensors every x seconds
        PCollection<Double> globallyGrouped = (PCollection<Double>)windowedForGlobalAverage
                .apply(Combine.globally(new AverageFn()).asSingletonView()).getPCollection();
        //  print current Stream
        if (globallyGrouped != null) {
            globallyGrouped.apply(ParDo.of(new PrintDoubleStream()));
        }


        pipeline.run().waitUntilFinish();
    }
}
