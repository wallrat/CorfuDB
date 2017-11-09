package org.corfudb.integration;

import org.corfudb.protocols.wireprotocol.orchestrator.AddNodeResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.OrchestratorResponse;
import org.corfudb.protocols.wireprotocol.orchestrator.QueryResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.MultiCheckpointWriter;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.collections.SMRMap;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Maithem on 12/1/17.
 */
public class AddNodeIT extends AbstractIT {

    final String host = "localhost";

    String getConnectionString(int port) {
        return host + ":" + port;
    }

    @Test
    public void AddNodeTest() throws Exception {
        final String host = "localhost";
        final String streamName = "s1";
        final int n1Port = 9000;

        // Start node one and populate it with data
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n1Port)
                .setSingle(true)
                .runServer();

        CorfuRuntime n1Rt = new CorfuRuntime(getConnectionString(n1Port)).connect();

        SMRMap<String, String> map = n1Rt.getObjectsView()
                .build()
                .setType(SMRMap.class)
                .setStreamName(streamName)
                .open();

        final int numEntris = 20_000;
        for (int x = 0; x < numEntris; x++) {
            map.put(String.valueOf(x), String.valueOf(x));

            if (x % 200 == 0) {
                System.out.println("iter " + x);
            }
        }

        // Add a second node
        final int n2Port = 9001;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n2Port)
                .runServer();

        ManagementClient mgmt = n1Rt.getRouter(getConnectionString(n1Port))
                .getClient(ManagementClient.class);

        OrchestratorResponse orchResp = mgmt.addNodeRequest(getConnectionString(n2Port)).get();
        AddNodeResponse resp = (AddNodeResponse) orchResp.getResponse();
        UUID workflowId = resp.getWorkflowId();

        boolean status = false;
        int maxTries = 10;

        for (int x = 0; x < maxTries; x++) {
            try {
                orchResp = mgmt.queryRequest(workflowId).get();
                status = ((QueryResponse) orchResp.getResponse()).isActive();
                System.out.println("Status for " + workflowId + " " + status);
                Thread.sleep(2000);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof WrongEpochException) {
                    n1Rt.invalidateLayout();
                } else {
                    throw e;
                }
            }
        }

        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(2);

        MultiCheckpointWriter mcw = new MultiCheckpointWriter();
        mcw.addMap(map);

        long prefix = mcw.appendCheckpoints(n1Rt, "Maithem");

        n1Rt.getAddressSpaceView().prefixTrim(prefix - 1);

        n1Rt.getAddressSpaceView().invalidateClientCache();
        n1Rt.getAddressSpaceView().invalidateServerCaches();
        n1Rt.getAddressSpaceView().gc();

        // Add a 3rd node after compaction
        final int n3Port = 9002;
        new CorfuServerRunner()
                .setHost(host)
                .setPort(n3Port)
                .runServer();

        orchResp = mgmt.addNodeRequest(getConnectionString(n3Port)).get();
        resp = (AddNodeResponse) orchResp.getResponse();
        workflowId = resp.getWorkflowId();

        for (int x = 0; x < maxTries; x++) {
            try {
                orchResp = mgmt.queryRequest(workflowId).get();
                status = ((QueryResponse) orchResp.getResponse()).isActive();
                System.out.println("Status for " + workflowId + " " + status);
                Thread.sleep(2000);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof WrongEpochException) {
                    n1Rt.invalidateLayout();
                } else {
                    throw e;
                }
            }
        }

        // Verify that the third node has been added and data can be read back
        n1Rt.invalidateLayout();
        assertThat(n1Rt.getLayoutView().getLayout().getAllServers().size()).isEqualTo(3);
        for (int x = 0; x < numEntris; x++) {
            String v = map.get(String.valueOf(x));
            assertThat(v).isEqualTo(String.valueOf(x));
        }

        System.out.println("printing");
    }
}


// create node, add new node, cp, add new node, verify data
// test case where holes need to be filled