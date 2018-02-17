package org.corfudb.runtime.view.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.ILogData;
import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AppendException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.runtime.exceptions.StaleTokenException;
import org.corfudb.runtime.exceptions.TrimmedException;
import org.corfudb.runtime.view.Address;

import java.util.Collections;
import java.util.List;
import java.util.NavigableSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.function.Function;

@Slf4j
public class SimpleView implements IStreamView {


    private NavigableSet<Long> addresses;

    private CorfuRuntime runtime;

    private long currentPtr;

    private UUID id;

    public SimpleView(UUID id, CorfuRuntime runtime) {
        this.runtime = runtime;
        this.id = id;
        this.addresses = new TreeSet<>();
    }

    @Override
    public UUID getId() {
        return id;
    }


    public void reset() {
        addresses.clear();
        currentPtr = Address.NON_ADDRESS;
    }


    @Override
    public void seek(long globalAddress) {
        currentPtr = globalAddress - 1;
    }


    @Override
    public long find(long globalAddress, SearchDirection direction) {
        throw new UnsupportedOperationException("find not supported in simple view");
    }

    @Override
    public long append(Object object,
                       Function<TokenResponse, Boolean> acquisitionCallback,
                       Function<TokenResponse, Boolean> deacquisitionCallback) {
        // First, we get a token from the sequencer.
        TokenResponse tokenResponse = runtime.getSequencerView()
                .nextToken(Collections.singleton(id), 1);

        // We loop forever until we are interrupted, since we may have to
        // acquire an address several times until we are successful.
        for (int x = 0; x < runtime.getParameters().getWriteRetry(); x++) {
            // Next, we call the acquisitionCallback, if present, informing
            // the client of the token that we acquired.
            if (acquisitionCallback != null) {
                if (!acquisitionCallback.apply(tokenResponse)) {
                    // The client did not like our token, so we end here.
                    // We'll leave the hole to be filled by the client or
                    // someone else.
                    log.debug("Acquisition rejected token={}", tokenResponse);
                    return -1L;
                }
            }

            // Now, we do the actual write. We could get an overwrite
            // exception here - any other exception we should pass up
            // to the client.
            try {
                runtime.getAddressSpaceView()
                        .write(tokenResponse, object);
                // The write completed successfully, so we return this
                // address to the client.
                return tokenResponse.getToken().getTokenValue();
            } catch (OverwriteException oe) {
                log.trace("Overwrite occurred at {}", tokenResponse);
                // We got overwritten, so we call the deacquisition callback
                // to inform the client we didn't get the address.
                if (deacquisitionCallback != null) {
                    if (!deacquisitionCallback.apply(tokenResponse)) {
                        log.debug("Deacquisition requested abort");
                        return -1L;
                    }
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                                1);
            } catch (StaleTokenException te) {
                log.trace("Token grew stale occurred at {}", tokenResponse);
                if (deacquisitionCallback != null && !deacquisitionCallback.apply(tokenResponse)) {
                    log.debug("Deacquisition requested abort");
                    return -1L;
                }
                // Request a new token, informing the sequencer we were
                // overwritten.
                tokenResponse = runtime.getSequencerView()
                        .nextToken(Collections.singleton(id),
                                1);

            }
        }

        log.error("append[{}]: failed after {} retries, write size {} bytes",
                tokenResponse.getTokenValue(),
                runtime.getParameters().getWriteRetry(),
                ILogData.getSerializedSize(object));
        throw new AppendException();
    }


    @Override
    public ILogData previous() {
        Long prevAddr = addresses.lower(currentPtr);

        if (prevAddr == null) {
            currentPtr = Address.NON_ADDRESS;
            return null;
        } else {
            currentPtr = prevAddr;
            return read(currentPtr);
        }
    }

    @Override
    public ILogData current() {
        if (Address.nonAddress(currentPtr)) {
            return null;
        }
        return read(currentPtr);
    }


    @Override
    public ILogData nextUpTo(long maxGlobal) {
        resolveStream(maxGlobal);
        Long nextAddr = addresses.higher(currentPtr);

        if (nextAddr == null) {
            return null;
        } else {
            currentPtr = nextAddr;
            return read(currentPtr);
        }
    }


    @Override
    public List<ILogData> remainingUpTo(long maxGlobal) {
        return null;
    }


    @Override
    public boolean hasNext() {
        return addresses.higher(currentPtr) != null;
    }

    @Override
    public long getCurrentGlobalPosition() {
        return currentPtr;
    }

    void resolveStream(long tail) {

        if (addresses.last() != null && tail <= addresses.last()) {
            // all entries are resolved
            return;
        }

        long stopAddress;

        if (addresses.last() == null) {
            stopAddress = Address.NON_ADDRESS;
        } else {
            stopAddress = addresses.last();
        }

        long currentAddress = tail;

        while (currentAddress > stopAddress && Address.isAddress(currentAddress)) {
            // Read the current address
            ILogData d;
            d = read(currentAddress);
            // If it contains the stream we are interested in
            if (d.containsStream(id)) {
                addresses.add(currentAddress);
            }

            currentAddress = d.getBackpointer(id);
        }
    }

    @Override
    public ILogData next() {

        Long nextAddr = addresses.higher(currentPtr);

        if (nextAddr == null) {
            // do sequencer call and retry
            long streamTail = runtime.getSequencerView()
                    .nextToken(Collections.singleton(id), 0).getToken().getTokenValue();

            resolveStream(streamTail);

            Long nextAddrAfterResolution = addresses.higher(currentPtr);
            if (nextAddrAfterResolution == null) {
                return null;
            } else {
                currentPtr = nextAddrAfterResolution;
                return read(nextAddrAfterResolution);
            }
        } else {
            currentPtr = nextAddr;
            return read(currentPtr);
        }
    }


    public List<ILogData> remaining() {
        return remainingUpTo(Address.MAX);
    }


    protected ILogData read(final long address) {
        try {
            return runtime.getAddressSpaceView().read(address);
        } catch (TrimmedException te) {
            throw te;
        }
    }

}
