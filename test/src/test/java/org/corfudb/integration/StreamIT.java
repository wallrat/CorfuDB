package org.corfudb.integration;

import com.carrotsearch.sizeof.RamUsageEstimator;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.stream.IStreamView;
import org.junit.Before;
import org.junit.Test;

import java.util.NavigableSet;
import java.util.Random;
import java.util.TreeSet;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {
   @Test
    public void simpleStreamTest() throws Exception {

       final NavigableSet<Long> readQ
               = new TreeSet<>();

       for (long x = 0; x < 500_000; x++) {
           readQ.add(x);
       }


       System.out.println(RamUsageEstimator.sizeOf(readQ));
    }
}
