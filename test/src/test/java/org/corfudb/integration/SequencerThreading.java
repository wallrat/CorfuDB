package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.junit.Test;

import java.util.Collections;

/**
 * Created by box on 12/13/17.
 */
public class SequencerThreading {

    @Test
    public void seqThread() throws Exception {

        CorfuRuntime rt = new CorfuRuntime("localhost:9000").connect();

        final int numThreads = 64;
        Thread[] threads = new Thread[numThreads];
        final int reqPerThread = 30000;

        for (int x = 0; x < numThreads; x++) {
            Runnable r = () -> {
                for (int i = 0; i < reqPerThread; i++) {
                    final int numTokens = 0;
                    TokenResponse tokenResponse =
                            rt.getSequencerView().nextToken(Collections.EMPTY_SET, numTokens);
                }
            };
            threads[x] = new Thread(r);
        }

        long start = System.currentTimeMillis();
        for (int x = 0; x < numThreads; x++) {
            threads[x].start();
        }

        for (int x = 0; x < numThreads; x++) {
            threads[x].join();
        }

        long end = System.currentTimeMillis();

        long diff = end - start;

        final double msInSec = 1000.0;
        System.out.println("Ops/Sec " + (((double) numThreads * reqPerThread) / diff) * msInSec);
    }
}
