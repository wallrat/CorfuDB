package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.BalancedMap;
import org.corfudb.runtime.collections.ISMRMap;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {

    @Test
    public void simpleStreamTest() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        final int numInstasnces = 1;
        ISMRMap<String,  String> map;

        //map = new BalancedMap<>(rt, numInstasnces, "map1");



        map = rt.getObjectsView().build()
                .setStreamName("map2")
                .setType(SMRMap.class)
                .open();



        final int numThreads = 4;
        final int iter = 10;
        final int upper = 500_000;

        Thread[] threads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            Runnable r = () -> {
                Random rand = new Random();

                for (int i = 0; i < iter; i++) {
                    int  n = rand.nextInt(upper) + 1;
                    int  d = rand.nextInt(10) + 1;
                    if (true) {
                        rt.getObjectsView().TXBegin();
                        map.put(String.valueOf(n), String.valueOf(n));
                        rt.getObjectsView().TXEnd();
                    } else {
                        TokenResponse tokenResponse =
                                rt.getSequencerView().nextToken(Collections.EMPTY_SET, 0);
                        long globalTail = tokenResponse.getToken().getTokenValue();
                        rt.getObjectsView().TXBuild().setSnapshot(Math.max(0, globalTail - 1)).begin();
                        map.get("n");
                        rt.getObjectsView().TXEnd();
                    }
                }
            };

            threads[x] = new Thread(r);
        }

        long start_time = System.nanoTime();
        for (int x = 0; x < numThreads; x++) {
            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }
        long end_time = System.nanoTime();
        double difference = (end_time - start_time) / 1e6;

        System.out.print(difference + " ms");
    }
}
