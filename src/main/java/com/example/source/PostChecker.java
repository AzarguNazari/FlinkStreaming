package com.example.source;


import com.example.Generator;
import com.example.event.BankAccount;
import com.example.event.Post;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.IntStream;

public class PostChecker extends RichSourceFunction<Post> {

    private boolean running = true;
    private AtomicLong postID = new AtomicLong(100);

    @Override
    public void run(SourceContext<Post> sourceContext) throws Exception {
        while (this.running) {
            final ThreadLocalRandom random = ThreadLocalRandom.current();
            long timestamp = System.currentTimeMillis();
            Post post = Generator.postGenerator(postID.get(), 50);
            // put generated sensor data to the queue
            sourceContext.collect(post);
            // sleep every one second after generating the fictional sensor data
            Thread.sleep(100);
        }

    }

    @Override
    public void cancel() {
        this.running = false;
    }
}