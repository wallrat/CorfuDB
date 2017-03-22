package org.corfudb.infrastructure;

import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;


import com.codahale.metrics.MetricRegistry;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import io.netty.channel.ChannelHandlerContext;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import org.corfudb.infrastructure.log.InMemoryStreamLog;
import org.corfudb.infrastructure.log.LogAddress;
import org.corfudb.infrastructure.log.StreamLog;
import org.corfudb.infrastructure.log.StreamLogFiles;
import org.corfudb.protocols.wireprotocol.CommitRequest;
import org.corfudb.protocols.wireprotocol.CorfuMsg;
import org.corfudb.protocols.wireprotocol.CorfuMsgType;
import org.corfudb.protocols.wireprotocol.CorfuPayloadMsg;
import org.corfudb.protocols.wireprotocol.DataType;
import org.corfudb.protocols.wireprotocol.IMetadata;
import org.corfudb.protocols.wireprotocol.LogData;
import org.corfudb.protocols.wireprotocol.ReadRequest;
import org.corfudb.protocols.wireprotocol.ReadResponse;
import org.corfudb.protocols.wireprotocol.TrimRequest;
import org.corfudb.protocols.wireprotocol.WriteMode;
import org.corfudb.protocols.wireprotocol.WriteRequest;
import org.corfudb.runtime.exceptions.DataCorruptionException;
import org.corfudb.runtime.exceptions.OverwriteException;
import org.corfudb.util.MetricsUtils;
import org.corfudb.util.Utils;
import org.corfudb.util.retry.IRetry;
import org.corfudb.util.retry.IntervalAndSentinelRetry;

import static org.corfudb.infrastructure.ServerContext.SMALL_INTERVAL;

/**
 * Created by mwei on 12/10/15.
 * <p>
 * A Log Unit Server, which is responsible for providing the persistent storage for the Corfu Distributed Shared Log.
 * <p>
 * All reads and writes go through a cache. For persistence, every 10,000 log entries are written to individual
 * files (logs), which are represented as FileHandles. Each FileHandle contains a pointer to the tail of the file, a
 * memory-mapped file channel, and a set of addresses known to be in the file. To append an entry, the pointer to the
 * tail is first extended to the length of the entry, and the entry is added to the set of known addresses. A header
 * is written, which consists of the ASCII characters LE, followed by a set of flags, the log unit address, the size
 * of the entry, then the metadata size, metadata and finally the entry itself. When the entry is complete, a written
 * flag is set in the flags field.
 */
@Slf4j
public class LogUnitServer extends AbstractServer {

    private final ServerContext serverContext;

    /**
     * A scheduler, which is used to schedule periodic tasks like garbage collection.
     */
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(
                    1,
                    new ThreadFactoryBuilder()
                            .setDaemon(true)
                            .setNameFormat("LogUnit-Maintenance-%d")
                            .build());

    /**
     * GC parameters
     * TODO: entire GC handling needs updating, currently not being activated
     */
    private final Thread gcThread = null;
    private IntervalAndSentinelRetry gcRetry;
    private AtomicBoolean running = new AtomicBoolean(true);

    /**
     * The options map.
     */
    private final Map<String, Object> opts;

    /**
     * Handler for the base server
     */
    @Getter
    private CorfuMsgHandler handler = new CorfuMsgHandler()
            .generateHandlers(MethodHandles.lookup(), this);

    /**
     * This cache services requests for data at various addresses. In a memory implementation,
     * it is not backed by anything, but in a disk implementation it is backed by persistent storage.
     */
    private final LoadingCache<LogAddress, LogData> dataCache;
    private final long maxCacheSize;

    private final StreamLog streamLog;

    private final BatchWriter<LogAddress, LogData> batchWriter;

    private static final String metricsPrefix = "corfu.server.logunit.";

    public LogUnitServer(ServerContext serverContext) {
        this.opts = serverContext.getServerConfig();
        this.serverContext = serverContext;
        maxCacheSize = Utils.parseLong(opts.get("--max-cache"));

        if ((Boolean) opts.get("--memory")) {
            log.warn("Log unit opened in-memory mode (Maximum size={}). " +
                    "This should be run for testing purposes only. " +
                    "If you exceed the maximum size of the unit, old entries will be AUTOMATICALLY trimmed. " +
                    "The unit WILL LOSE ALL DATA if it exits.", Utils.convertToByteStringRepresentation(maxCacheSize));
            streamLog = new InMemoryStreamLog();
        } else {
            String logdir = opts.get("--log-path") + File.separator + "log";
            File dir = new File(logdir);
            if (!dir.exists()) {
                dir.mkdir();
            }
            streamLog = new StreamLogFiles(logdir, (Boolean) opts.get("--no-verify"));
        }

        batchWriter = new BatchWriter(streamLog);

        dataCache = Caffeine.<LogAddress, LogData>newBuilder()
                .<LogAddress, LogData>weigher((k, v) -> v.getData() == null ? 1 : v.getData().length)
                .maximumWeight(maxCacheSize)
                .removalListener(this::handleEviction)
                .writer(batchWriter)
                .recordStats()
                .build(this::handleRetrieval);

        MetricRegistry metrics = serverContext.getMetrics();
        MetricsUtils.addCacheGauges(metrics, metricsPrefix + "cache.", dataCache);
    }

    /**
     * Service an incoming write request.
     */
    @ServerHandler(type = CorfuMsgType.WRITE, opTimer = metricsPrefix + "write")
    public void write(CorfuPayloadMsg<WriteRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                      boolean isMetricsEnabled) {
        log.debug("log write: global: {}, streams: {}, backpointers: {}", msg
                .getPayload().getGlobalAddress(),
                msg.getPayload().getStreamAddresses(), msg.getPayload().getData().getBackpointerMap());
        // clear any commit record (or set initially to false).
        msg.getPayload().clearCommit();
        try {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM) {
                dataCache.put(new LogAddress(msg.getPayload().getGlobalAddress(), null), msg.getPayload().getData());
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
                return;
            } else {
                for (UUID streamID : msg.getPayload().getStreamAddresses().keySet()) {
                    dataCache.put(new LogAddress(msg.getPayload().getStreamAddresses().get(streamID), streamID),
                            msg.getPayload().getData());
                }
                r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());
            }
        } catch (OverwriteException ex) {
            if (msg.getPayload().getWriteMode() != WriteMode.REPLEX_STREAM)
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
            else
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_REPLEX_OVERWRITE.msg());
        }
    }

    /**
     * Service an incoming commit request.
     */
    @ServerHandler(type = CorfuMsgType.COMMIT, opTimer = metricsPrefix + "commit")
    public void commit(CorfuPayloadMsg<CommitRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                       boolean isMetricsEnabled) {
        Map<UUID, Long> streamAddresses = msg.getPayload().getStreams();
        if (streamAddresses == null) {
            // Then this is a commit bit for the global log.
            LogData entry = dataCache.get(new LogAddress(msg.getPayload().getAddress(), null));
            if (entry == null) {
                r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                return;
            } else {
                entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
            }
        } else {
            for (UUID streamID : msg.getPayload().getStreams().keySet()) {
                LogData entry = dataCache.get(new LogAddress(streamAddresses.get(streamID), streamID));
                if (entry == null) {
                    r.sendResponse(ctx, msg, CorfuMsgType.ERROR_NOENTRY.msg());
                    // TODO: Crap, we have to go back and undo all the commit bits??
                    return;
                } else {
                    entry.getMetadataMap().put(IMetadata.LogUnitMetadataType.COMMIT, msg.getPayload().getCommit());
                }
            }
        }
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.READ_REQUEST, opTimer = metricsPrefix + "read")
    private void read(CorfuPayloadMsg<ReadRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                      boolean isMetricsEnabled) {
        log.trace("log read: {} {}", msg.getPayload().getStreamID()  == null
                        ? "global" : msg.getPayload().getStreamID(),
                msg.getPayload().getRange());
        ReadResponse rr = new ReadResponse();
        try {
            for (Long l = msg.getPayload().getRange().lowerEndpoint();
                 l < msg.getPayload().getRange().upperEndpoint() + 1L; l++) {
                LogAddress logAddress = new LogAddress(l, msg.getPayload().getStreamID());
                LogData e = dataCache.get(logAddress);
                if (e == null) {
                    rr.put(l, LogData.EMPTY);
                } else if (e.getType() == DataType.HOLE) {
                    rr.put(l, LogData.HOLE);
                } else {
                    rr.put(l, e);
                }
            }
            r.sendResponse(ctx, msg, CorfuMsgType.READ_RESPONSE.payloadMsg(rr));
        } catch (DataCorruptionException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_DATA_CORRUPTION.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.GC_INTERVAL, opTimer = metricsPrefix + "gc-interval")
    private void setGcInterval(CorfuPayloadMsg<Long> msg, ChannelHandlerContext ctx, IServerRouter r,
                               boolean isMetricsEnabled) {
        gcRetry.setRetryInterval(msg.getPayload());
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.FORCE_GC, opTimer = metricsPrefix + "force-gc")
    private void forceGc(CorfuMsg msg, ChannelHandlerContext ctx, IServerRouter r,
                         boolean isMetricsEnabled) {
        gcThread.interrupt();
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    @ServerHandler(type = CorfuMsgType.FILL_HOLE, opTimer = metricsPrefix + "fill-hole")
    private void fillHole(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r,
                          boolean isMetricsEnabled) {
        try {
            dataCache.put(new LogAddress(msg.getPayload().getPrefix(), msg.getPayload().getStream()), LogData.HOLE);
            r.sendResponse(ctx, msg, CorfuMsgType.WRITE_OK.msg());

        } catch (OverwriteException e) {
            r.sendResponse(ctx, msg, CorfuMsgType.ERROR_OVERWRITE.msg());
        }
    }

    @ServerHandler(type = CorfuMsgType.TRIM)
    private void trim(CorfuPayloadMsg<TrimRequest> msg, ChannelHandlerContext ctx, IServerRouter r) {
        batchWriter.trim(new LogAddress(msg.getPayload().getPrefix(), msg.getPayload().getStream()));
        //TODO(Maithem): should we return an error if the write fails
        r.sendResponse(ctx, msg, CorfuMsgType.ACK.msg());
    }

    /**
     * Retrieve the LogUnitEntry from disk, given an address.
     *
     * @param logAddress The address to retrieve the entry from.
     * @return The log unit entry to retrieve into the cache.
     * This function should not care about trimmed addresses, as that is handled in
     * the read() and append(). Any address that cannot be retrieved should be returned as
     * unwritten (null).
     */
    public synchronized LogData handleRetrieval(LogAddress logAddress) {
        LogData entry = streamLog.read(logAddress);
        log.trace("Retrieved[{} : {}]", logAddress, entry);
        return entry;
    }


    public synchronized void handleEviction(LogAddress logAddress, LogData entry, RemovalCause cause) {
        log.trace("Eviction[{}]: {}", logAddress, cause);
        streamLog.release(logAddress, entry);
    }

    /**
     * Shutdown the server.
     */
    @Override
    public void shutdown() {
        scheduler.shutdownNow();
        batchWriter.close();
    }

    @VisibleForTesting
    LoadingCache<LogAddress, LogData> getDataCache() {
        return dataCache;
    }
}