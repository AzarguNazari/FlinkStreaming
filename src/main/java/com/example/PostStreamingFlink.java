package com.example;

import com.example.event.Post;
import com.example.source.PostChecker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;


public class PostStreamingFlink {

    public static void main(String[] args) throws Exception {

//        System.getProperties().put("log4j.rootLogger", "INFO, console");
//        System.getProperties().put("log4j.appender.console", "org.apache.log4j.ConsoleAppender");
//        System.getProperties().put("log4j.appender.console.layout", "org.apache.log4j.PatternLayout");
//        System.getProperties().put("log4j.appender.console.layout.ConversionPattern", "%d{HH:mm:ss,SSS} %-5p %-60c %x - %m%n");


        // set up the streaming execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set time characteristic to be com.example.event timestamp
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // assign fictious stream of data as our data com.example.source and extract timestamp field
        DataStream<Post> inputStream = env.addSource(new PostChecker()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Post>() {
            @Override
            public long extractAscendingTimestamp(Post post) {
                return post.getTimestamp();
            }
        });

        DataStream<Post> postContents = inputStream.map(new MapFunction<Post, Post>() {
            @Override
            public Post map(Post post) throws Exception {
                return post;
            }
        });

        DataStream<Long> checkContent = postContents.filter(post  -> post.getPostDescription().contains("fuck") || post.getPostDescription().contains("fake")).map(Post::getId).rebalance();

        checkContent.print("Post should be banned due to voilating rules");


        env.execute("Flink Streaming Java API Skeleton");
    }
}