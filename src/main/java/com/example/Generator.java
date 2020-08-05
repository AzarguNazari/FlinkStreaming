package com.example;

import com.example.event.Post;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;
import java.util.stream.IntStream;

public class Generator {

    private static Scanner scanner;
    static Object[] words = null;

    static {
        try {
            Set<String> wordDictionary = new HashSet<String>();
            Files.readAllLines(Paths.get("/home/hazargn/git/FlinkStreaming/src/main/resources/post.txt")).stream().forEach(line -> {
                Arrays.asList(line.trim().split(" +")).forEach(wordDictionary::add);
            });
            words = wordDictionary.toArray();
        }catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Post postGenerator(long id, int size){
        if(size < 0) throw new IllegalArgumentException("Range cannot be negative");
        if(size > words.length) throw new IllegalArgumentException("Range cannot be greather than " + words.length + " of word");
        StringBuilder postDescription = new StringBuilder();
        IntStream.rangeClosed(1, size).forEach(number -> {
            postDescription.append(words[(int)(Math.random() * words.length)]).append(" ");
        });
        return Post.builder().id(id).postDescription(postDescription.toString()).timestamp(System.currentTimeMillis()).build();
    }

}
