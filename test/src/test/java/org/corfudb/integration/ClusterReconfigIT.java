package org.corfudb.integration;

import java.util.Map;
import java.util.Random;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.collections.SMRMap;
import org.junit.Before;
import org.junit.Test;

public class ClusterReconfigIT extends AbstractIT {

    private static String corfuSingleNodeHost;

    @Before
    public void loadProperties() {
        corfuSingleNodeHost = (String) PROPERTIES.get("corfuSingleNodeHost");
    }

    private Random getRandomNumberGenerator() {
        final Random randomSeed = new Random();
        final long SEED = randomSeed.nextLong();
        // Keep this print at all times to reproduce any failed test.
        System.out.println("SEED = " + Long.toHexString(SEED));
        return new Random(SEED);
    }

    /**
     * Creates a message of specified size in bytes.
     *
     * @param msgSize
     * @return
     */
    private static String createStringOfSize(int msgSize) {
        StringBuilder sb = new StringBuilder(msgSize);
        for (int i = 0; i < msgSize; i++) {
            sb.append('a');
        }
        return sb.toString();
    }

    @Test
    public void addNodesTest() throws Exception {

        final int PORT_0 = 9000;
        final int PORT_1 = 9001;
        final int PORT_2 = 9002;

        Process corfuServer_1 = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(PORT_0)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, PORT_0))
                .setSingle(true)
                .runServer();

        Process corfuServer_2 = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(PORT_1)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, PORT_1))
                .setSingle(false)
                .runServer();

        Process corfuServer_3 = new CorfuServerRunner()
                .setHost(corfuSingleNodeHost)
                .setPort(PORT_2)
                .setLogPath(getCorfuServerLogPath(corfuSingleNodeHost, PORT_2))
                .setSingle(false)
                .runServer();

        CorfuRuntime runtime = createDefaultRuntime();

        Map<String, String> map = runtime.getObjectsView()
                .build()
                .setStreamName("test")
                .setType(SMRMap.class)
                .open();

        final String data = createStringOfSize(1_000);

        final int num = 15_000;

        Random r = getRandomNumberGenerator();
        for (int i = 0; i < num; i++) {
            String key = Long.toString(r.nextLong());
            map.put(key, data);
        }

        Thread t = new Thread(() -> {
            while (true) {
                try {
                    runtime.getObjectsView().TXBegin();
                    map.put(Integer.toString(r.nextInt()), data);
                    runtime.getObjectsView().TXEnd();
                } catch (Exception e) {
                }
            }
        });
        t.start();

        long startTime = System.currentTimeMillis();
        runtime.getManagementView().addNode("localhost:9001");
        runtime.getManagementView().addNode("localhost:9002");
        long endTime = System.currentTimeMillis() - startTime;

        final long retryTime = 500;
        final long epochAfter2Adds = 4L;
        while (runtime.getLayoutView().getLayout().getEpoch() != epochAfter2Adds) {
            Thread.sleep(retryTime);
            runtime.invalidateLayout();
        }
        System.out.println("COMPLETION TIME [" + endTime + "ms]");

        shutdownCorfuServer(corfuServer_1);
        shutdownCorfuServer(corfuServer_2);
        shutdownCorfuServer(corfuServer_3);
    }
}