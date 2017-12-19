package org.corfudb.runtime;

import lombok.Data;
import lombok.Getter;
import lombok.NonNull;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by box on 12/18/17.
 */
public class Tracer {

    @Getter
    static Tracer tracer = new Tracer();

    String start = "B";
    String end = "E";
    String cat = "client";

    Queue<Event> events = new ConcurrentLinkedQueue();

    public void startEvent(String name) {
        long pid = 0;
        long tid = Thread.currentThread().getId();
        long ts =  System.nanoTime();
        Event event = new Event(cat, pid, tid, ts, start, name);
        events.add(event);

    }

    public void endEvent(String name) {
        long pid = 0;
        long tid = Thread.currentThread().getId();
        long ts =  System.nanoTime();
        Event event = new Event(cat, pid, tid, ts, end, name);
        events.add(event);

    }

    public void dump(String path) throws Exception {
        String str = "[";
        while (events.peek() != null) {
            str += events.poll().getString() + ",\n";
        }

        str += "]\n";

        Path targetPath = Paths.get(path);
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        Files.write(targetPath, bytes, StandardOpenOption.CREATE);

    }

    @Data
    class Event {
        @NonNull
        String cat;

        @NonNull
        long pid;

        @NonNull
        long tid;

        @NonNull
        long ts;

        @NonNull
        String ph;

        @NonNull
        String name;

        public String getString() {
            String str = "";

            str += "{";
            str += "\"cat\": \"" + cat + "\",";
            str += "\"pid\": " + pid + ",";
            str += "\"tid\": " + tid + ",";
            str += "\"ts\": " + ts + ",";
            str += "\"ph\": \"" + ph + "\",";
            str += "\"name\": \"" + name + "\",";
            str += "\"args\": {}";
            str += "}";

            return str;
        }
    }
}
