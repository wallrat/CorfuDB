package org.corfudb.integration;

import com.google.common.reflect.TypeToken;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.CorfuTable;
import org.corfudb.runtime.collections.CorfuTableTest;
import org.corfudb.runtime.object.transactions.TransactionType;
import org.junit.Test;

import static org.corfudb.integration.AbstractIT.createDefaultRuntime;

/**
 * A set integration tests that exercise the stream API.
 */

public class StreamIT {

    //@Test
    public void RunLoad() throws Exception {
        CorfuRuntime runtime = createDefaultRuntime();

        CorfuTable<String, String, CorfuTableTest.StringIndexers, String>
                corfuTable1 = runtime.getObjectsView().build()
                .setTypeToken(
                        new TypeToken<CorfuTable<String, String, CorfuTableTest.StringIndexers, String>>() {})
                .setArguments(CorfuTableTest.StringIndexers.class)
                .setStreamName("test")
                .open();

        long t1 = System.currentTimeMillis();
        final int REPETITIONS = 10_000;
        final int entriesPerTxn = 10;
        Thread th1 = new Thread(() -> {
            for(int i = 0; i < REPETITIONS; i++) {
                TXBegin(runtime);
                for (int x = 0; x < entriesPerTxn; x++) {
                    corfuTable1.put("keya" + i, "vala" + i);
                }
                TXEnd(runtime);
            }
        });
        Thread th2 = new Thread(() -> {
            for(int i = 0; i < REPETITIONS; i++) {
                //TXBegin(runtime);
                corfuTable1.put("keya" + i, "vala" + i);
                //TXEnd(runtime);
            }
        });
        th1.start();
        th2.start();
        th1.join();
        th2.join();

        System.out.println("Time Taken: " + (System.currentTimeMillis() - t1));



    }

    private void TXBegin(CorfuRuntime rt) {
        rt.getObjectsView().TXBuild()
                .setType(TransactionType.OPTIMISTIC)
                .begin();
    }

    private void TXEnd(CorfuRuntime rt) {
        rt.getObjectsView().TXEnd();
    }

}
