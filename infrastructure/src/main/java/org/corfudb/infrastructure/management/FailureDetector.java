package org.corfudb.infrastructure.management;

import java.util.*;
import java.util.concurrent.CompletableFuture;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.clients.BaseClient;
import org.corfudb.runtime.clients.IClientRouter;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.runtime.view.Layout;

/**
 * FailureDetector polls all the "responsive members" in the layout.
 * Responsive members: All endpoints excluding the unresponsiveServers list.
 * For every poll method call invoked, it starts a polling round and generates a poll report.
 * Each polling round comprises of "failureThreshold" number of iterations.
 * - We asynchronously poll every known responsive member in the layout.
 * - Poll result aggregation.
 * - If we complete an iteration without detecting failures, we end the round successfully.
 * The management Server ensures only one instance of this class and hence this is NOT thread safe.
 * Created by zlokhandwala on 11/29/17.
 */
@Slf4j
public class FailureDetector implements IDetector {

    /**
     * Members to poll in every round
     */
    private String[] members;

    /**
     * Response timeout for every router.
     */
    @Getter
    private long period;

    /**
     * Number of iterations to execute to detect a failure in a round.
     */
    @Getter
    @Setter
    private int failureThreshold = 3;

    /**
     * Max duration for the responseTimeouts of the routers.
     */
    @Getter
    @Setter
    private long maxPeriodDuration = 8_000L;

    /**
     * Min duration for the responseTimeouts of the routers.
     */
    @Getter
    @Setter
    private long initPeriodDuration = 5_000L;

    /**
     * Interval between iterations in a pollRound.
     */
    @Getter
    @Setter
    private long interIterationInterval = 1_000;

    /**
     * Increments in which the period moves towards the maxPeriodDuration in every failed
     * iteration.
     */
    @Getter
    @Setter
    private long periodDelta = 1_000L;

    private int[] responses;
    private IClientRouter[] membersRouters;
    private CompletableFuture<Boolean>[] pollCompletableFutures = null;

    private long[] expectedEpoch;
    private final long INVALID_EPOCH = -1L;
    private final int INVALID_RESPONSE = -1;
    private long currentEpoch = INVALID_EPOCH;

    /**
     * Executes the policy once.
     * Checks for changes in the layout.
     * Then polls all the servers generates pollReport.
     *
     * @param layout Current Layout
     */
    public PollReport poll(Layout layout, CorfuRuntime corfuRuntime) {

        // Performs setup and checks for changes in the layout to update failure count
        setup(layout, corfuRuntime);

        // Perform polling of all responsive servers.
        return pollRound();

    }

    /**
     * Triggered on every epoch change.
     * Sets up the responsive servers array. (All servers excluding unresponsive servers)
     * Resets the currentEpoch to the layout epoch.
     *
     * @param layout       Latest known layout
     * @param corfuRuntime Connected CorfuRuntime instance.
     */
    private void setup(Layout layout, CorfuRuntime corfuRuntime) {

        if (currentEpoch != layout.getEpoch()) {

            // Reset local copy of the epoch.
            currentEpoch = layout.getEpoch();

            // Collect and set all responsive servers in the members array.
            Set<String> allResponsiveServersSet = layout.getAllServers();
            allResponsiveServersSet.removeAll(layout.getUnresponsiveServers());
            members = allResponsiveServersSet.toArray(new String[allResponsiveServersSet.size()]);
            Arrays.sort(members);

            log.debug("Responsive members to poll, {}", new ArrayList<>(Arrays.asList(members)));

            // Set up arrays for routers to the endpoints.
            membersRouters = new IClientRouter[members.length];
            responses = new int[members.length];
            expectedEpoch = new long[members.length];
            period = initPeriodDuration;

            for (int i = 0; i < members.length; i++) {
                membersRouters[i] = corfuRuntime.getRouterFunction.apply(members[i]);
                membersRouters[i].setTimeoutResponse(period);
                expectedEpoch[i] = INVALID_EPOCH;
            }

            pollCompletableFutures = new CompletableFuture[members.length];

        } else {
            log.debug("No server list change since last poll.");
        }
    }

    /**
     * Poll all members servers once asynchronously and store their futures in
     * pollCompletableFutures.
     */
    private void pollOnceAsync() {
        // Poll servers for health.  All ping activity will happen in the background.
        for (int i = 0; i < members.length; i++) {
            try {
                pollCompletableFutures[i] = membersRouters[i].getClient(BaseClient.class).ping();
            } catch (Exception e) {
                CompletableFuture<Boolean> cf = new CompletableFuture<>();
                cf.completeExceptionally(e);
                pollCompletableFutures[i] = cf;
            }
        }
    }

    /**
     * Reset all responses to an invalid iteration number, -1.
     */
    private void resetResponses() {
        for (int i = 0; i < responses.length; i++) {
            responses[i] = INVALID_RESPONSE;
        }
    }

    /**
     * Block on all poll futures and collect the responses. There are 3 possible cases.
     * 1. Receive a PONG. We set the responses[i] to the polling iteration number.
     * 2. WrongEpochException is thrown. We mark as a successful response but also record the
     * expected epoch.
     * 3. Other Exception is thrown. We do nothing. (The response[i] in this case is left behind.)
     *
     * @param pollIteration poll iteration in the ongoing polling round.
     */
    private void collectResponsesAndVerifyEpochs(int pollIteration) {
        // Collect responses and increment response counters for successful pings.
        for (int i = 0; i < members.length; i++) {
            try {
                pollCompletableFutures[i].get();
                responses[i] = pollIteration;
                expectedEpoch[i] = INVALID_EPOCH;
            } catch (Exception e) {
                if (e.getCause() instanceof WrongEpochException) {
                    responses[i] = pollIteration;
                    expectedEpoch[i] = ((WrongEpochException) e.getCause()).getCorrectEpoch();
                }
            }
        }
    }

    /**
     * This method performs 2 tasks:
     * 1. Aggregates the successful responses in the responsiveNodes Set.
     * If the response was unsuccessful we increment the timeout period.
     * 2. Checks if there were no failures and no wrongEpochExceptions in which case the ongoing
     * polling round is completed. However, if failures were present (not epoch errors), then all
     * the routers' response timeouts are updated with the increased new period and the round
     * continues.
     *
     * @param membersSet    Set of member servers.
     * @param pollIteration Iteration of the ongoing polling round.
     * @return True if no failures were present. False otherwise.
     */
    private boolean isFailurePresent(Set<String> membersSet, int pollIteration) {

        // Aggregate the responses.
        Set<String> responsiveNodes = new HashSet<>();
        long newPeriod = 0;
        for (int i = 0; i < members.length; i++) {
            // If this counter is left behind and this node is not in members
            if (membersSet.contains(members[i]) && responses[i] != pollIteration) {
                // The existing router response timeout is increased.
                newPeriod = getIncreasedPeriod();
            } else {
                responsiveNodes.add(members[i]);
            }
        }

        // We received responses from all the members.
        if (responsiveNodes.equals(membersSet)) {
            // We can try to scale back the network latency time only if all pings received.
            period = Math.max(initPeriodDuration, (period - periodDelta));
            // If there are no ping failures and no out of phase epochs then end round.
            if (Arrays.stream(expectedEpoch).filter(l -> l != INVALID_EPOCH).count() == 0) {
                return false;
            }

        } else {
            if (newPeriod != period && newPeriod != 0) {
                period = newPeriod;
                tuneRoutersResponseTimeout(membersSet, period);
            }
        }
        return true;
    }

    /**
     * Each PollRound executes "failureThreshold" number of iterations and generates the
     * poll report.
     *
     * @return Poll Report with detected failed nodes and out of phase epoch nodes.
     */
    private PollReport pollRound() {

        Set<String> membersSet = new HashSet<>(Arrays.asList(members));
        boolean failuresDetected = true;

        // At the start of the round reset all response counters.
        resetResponses();

        // In each iteration we poll all the servers in the members list.
        for (int iteration = 1; iteration <= failureThreshold; iteration++) {

            // Ping all nodes and await their responses.
            pollOnceAsync();

            // Collect responses and increment response counters for successful pings.
            collectResponsesAndVerifyEpochs(iteration);

            failuresDetected = isFailurePresent(membersSet, iteration);
            if (!failuresDetected) {
                break;
            }

            try {
                Thread.sleep(interIterationInterval);
            } catch (InterruptedException e) {
                log.warn("pollRound: Sleep interrupted.");
            }

        }

        Map<String, Long> outOfPhaseEpochNodes = new HashMap<>();
        Set<String> failed = new HashSet<>();

        if (failuresDetected) {
            // Check all responses and collect all failures.
            for (int i = 0; i < members.length; i++) {
                if (responses[i] != failureThreshold) {
                    failed.add(members[i]);
                }
                if (expectedEpoch[i] != INVALID_EPOCH) {
                    outOfPhaseEpochNodes.put(members[i], expectedEpoch[i]);
                    expectedEpoch[i] = INVALID_EPOCH;
                }

                // Reset the timeout of all the failed nodes to the max value to set a longer
                // timeout period to detect their response.
                tuneRoutersResponseTimeout(failed, maxPeriodDuration);
            }
        } else {
            // Need to tune back if no failures.
            tuneRoutersResponseTimeout(membersSet, initPeriodDuration);
        }

        return new PollReport.PollReportBuilder()
                .pollEpoch(currentEpoch)
                .failingNodes(failed)
                .outOfPhaseEpochNodes(outOfPhaseEpochNodes)
                .build();
    }

    /**
     * Function to increment the existing response timeout period.
     *
     * @return The new calculated timeout value.
     */
    private long getIncreasedPeriod() {
        return Math.min(maxPeriodDuration, (period + periodDelta));
    }

    /**
     * Set the timeoutResponse for all the routers connected to the given endpoints with the
     * given value.
     *
     * @param endpoints Router endpoints.
     * @param timeout   New timeout value.
     */
    private void tuneRoutersResponseTimeout(Set<String> endpoints, long timeout) {
        for (int i = 0; i < members.length; i++) {
            // Change timeout delay of routers of members list.
            if (endpoints.contains(members[i])) {
                membersRouters[i].setTimeoutResponse(timeout);
            }
        }
    }
}
