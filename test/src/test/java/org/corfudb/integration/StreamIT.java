package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
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
        CorfuRuntime[] rt = new CorfuRuntime[4];
        rt[0] = new CorfuRuntime("10.33.83.31:9000").connect();
        rt[1] = new CorfuRuntime("10.33.83.31:9000").connect();
        rt[2] = new CorfuRuntime("10.33.83.31:9000").connect();
        rt[3] = new CorfuRuntime("10.33.83.31:9000").connect();
        

        final int numThreads = 20;
        final int numIter = 50000;
        Thread[] threads = new Thread[numThreads];

        for (int x = 0; x < numThreads; x++) {
            Runnable r = () -> {
                Random rand = new Random();
                for (int y = 0; y < numIter; y++) {
                    int tokens = 0;
                    rt[rand.nextInt(4)].getSequencerView().nextToken(Collections.emptySet(), tokens);
                }
            };

            threads[x] = new Thread(r);
        }

        long s1 = System.currentTimeMillis();
        for (int x = 0; x < numThreads; x++) {
            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }

        long s2 = System.currentTimeMillis();

        System.out.println("total time " + (s2 - s1));
        System.out.println("rpc/ms " + ((numIter * numThreads) / (s2 - s1)));
    }
}
