package org.corfudb.runtime.view;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.runtime.clients.NettyClientRouter;
import org.corfudb.util.CFUtils;

import java.util.List;

/**
 * A view of the Management Server to manage reconfigurations of the Corfu Cluster.
 *
 * <p>Created by zlokhandwala on 11/20/17.</p>
 */
@Slf4j
public class ManagementView extends AbstractView {

    private final long layoutRefreshTimeout = 500;

    public ManagementView(@NonNull CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Add a new node to the existing cluster.
     *
     * @param endpoint Endpoint of the new node to be added to the cluster.
     * @return True if completed successfully.
     */
    public boolean addNode(String endpoint) {
        return layoutHelper(l -> {

            System.out.println("Before sending message");

            try {
                System.out.println(runtime.getLayoutView().getLayout());
                System.out.println(l.getLayoutServers());
                List<String> serverList = l.getSegments().get(0).getStripes().get(0).getLogServers();
                String server = serverList.get(serverList.size()-1);
                System.out.println("Server we use for router");
                System.out.println(server);

                NettyClientRouter ncr = (NettyClientRouter) runtime.getRouter(server);
                System.out.println(ncr);
                System.out.println("Epoch of cmdlet router:");
                System.out.println(ncr.getEpoch());


                CFUtils.getUninterruptibly(runtime
                        .getRouter(server)
                        .getClient(ManagementClient.class)
                        .addNodeRequest(endpoint));
            } catch (Exception e) {
                e.printStackTrace();
                throw e;
            }

            System.out.println("After message sent");

            while (!runtime.getLayoutView().getLayout().getAllServers().contains(endpoint)
                    || runtime.getLayoutView().getLayout().getSegments().size() != 1) {
                System.out.println("Invalidate the layout");
                runtime.invalidateLayout();
                try {
                    Thread.sleep(layoutRefreshTimeout);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            return true;
        });
    }
}
