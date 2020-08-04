package source;


import event.BankAccount;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.text.DecimalFormat;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class TemperatureSensor extends RichSourceFunction<BankAccount> {

    private boolean running = true;
    private String BSZ;
    private Double balance;
    private String holderName;

    private AtomicInteger id = new AtomicInteger(0);
    private AtomicLong accountNumber = new AtomicLong(100);

    @Override
    public void run(SourceContext<BankAccount> sourceContext) throws Exception {
        while (this.running) {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            long timestamp = System.currentTimeMillis();
            BankAccount sensor = new BankAccount(id.getAndIncrement(), accountNumber.getAndIncrement(), generateBSZ(), Math.random() * 100000, "user" + id.get(), timestamp);
            // put generated sensor data to the queue
            sourceContext.collect(sensor);
            // sleep every one second after generating the fictional sensor data
            Thread.sleep(100);
        }

    }

    private static String generateBSZ(){
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DE");
        IntStream.rangeClosed(1, 14).forEach(num -> stringBuilder.append((int)(Math.random() * 10)));
        return stringBuilder.toString();
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}