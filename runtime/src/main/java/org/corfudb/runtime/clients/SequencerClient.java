package org.corfudb.runtime.clients;

import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.SequencerTailsRecoveryMsg;
import org.corfudb.protocols.wireprotocol.TokenRequest;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;


/**
 * A sequencer client.
 *
 * <p>This client allows the client to obtain sequence numbers from a sequencer.
 *
 * <p>Created by mwei on 12/10/15.
 */
public class SequencerClient extends AbstractClient {

    public SequencerClient(IClientRouter router, long epoch) {
        super(router, epoch);
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs Set of streamIDs to be assigned ot the token.
     * @param numTokens Number of tokens to be reserved.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens) {
        return sendMessageWithFuture(CorfuMsgType.TOKEN_REQ.payloadMsg(
                new TokenRequest(numTokens, streamIDs)));
    }

    /**
     * Fetches the next available token from the sequencer.
     *
     * @param streamIDs    Set of streamIDs to be assigned ot the token.
     * @param numTokens    Number of tokens to be reserved.
     * @param conflictInfo Transaction resolution conflict parameters.
     * @return A completable future with the token response from the sequencer.
     */
    public CompletableFuture<TokenResponse> nextToken(Set<UUID> streamIDs, long numTokens,
                                                      TxResolutionInfo conflictInfo) {
        return sendMessageWithFuture(CorfuMsgType.TOKEN_REQ.payloadMsg(
                new TokenRequest(numTokens, streamIDs, conflictInfo)));
    }

    public CompletableFuture<Void> trimCache(Long address) {
        return sendMessageWithFuture(CorfuMsgType.SEQUENCER_TRIM_REQ.payloadMsg(address));
    }

    /**
     * Resets the sequencer with the specified initialToken
     *
     * @param initialToken Token Number which the sequencer starts distributing.
     * @return A CompletableFuture which completes once the sequencer is reset.
     */
    public CompletableFuture<Boolean> bootstrap(Long initialToken, Map<UUID, Long> sequencerTails,
                                                Long readyStateEpoch) {
        return sendMessageWithFuture(CorfuMsgType.BOOTSTRAP_SEQUENCER.payloadMsg(
                new SequencerTailsRecoveryMsg(initialToken, sequencerTails, readyStateEpoch)));
    }
}
