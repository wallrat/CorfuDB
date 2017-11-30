package org.corfudb.runtime.view;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.ManagementClient;
import org.corfudb.util.CFUtils;

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

            CFUtils.getUninterruptibly(runtime
                    .getRouter(l.getLayoutServers().get(l.getLayoutServers().size() - 1))
                    .getClient(ManagementClient.class)
                    .addNodeRequest(endpoint));

            while (!runtime.getLayoutView().getLayout().getAllServers().contains(endpoint)
                    || runtime.getLayoutView().getLayout().getSegments().size() != 1) {
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
