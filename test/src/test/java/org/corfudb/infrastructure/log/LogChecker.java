package org.corfudb.infrastructure.log;

import lombok.Data;
import org.corfudb.infrastructure.ServerContext;
import org.corfudb.infrastructure.ServerContextBuilder;
import org.corfudb.protocols.wireprotocol.LogData;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Created by Maithem on 4/10/18.
 */
public class LogChecker {

    public void checkLog() {

        String logPathDir = "";
        ServerContext sc = new ServerContextBuilder()
                .setLogPath(logPathDir)
                .setMemory(false)
                .build();


        String luDir = logPathDir + File.separator + "log";
        File path = new File(luDir);
        File[] files = path.listFiles();

        List<String> segmentFiles = new ArrayList<>();

        for (File segFile : files) {
            if (segFile.getAbsolutePath().endsWith(".log")) {
                segmentFiles.add(segFile.getAbsolutePath());
            }
        }

        Collections.sort(segmentFiles);
        String tailSegmentPath = segmentFiles.get(segmentFiles.size() - 1);

        String[] tmpList = tailSegmentPath.split(File.separator);

        // get the largest segment
        String tailSegSuffix = tmpList[tmpList.length - 1];

        // remove .log
        long tailSegment = Long.valueOf(tailSegSuffix.substring(0, tailSegSuffix.length() - 4));
        sc.setStartingAddress(0);
        sc.setTailSegment(tailSegment);

        StreamLogFiles log = new StreamLogFiles(sc, false);

        for (long segment = tailSegment; segment >= 0; segment--) {
            SegmentHandle sh = log.getSegmentHandleForAddress(segment);
            List<Long> segmentAddresses = new ArrayList<>(sh.getKnownAddresses().keySet());
            Collections.sort(segmentAddresses);

            processSegment(segmentAddresses, log);
        }
    }

    void processSegment(List<Long> addresses, StreamLog log) {

        for (int x = addresses.size() - 1; x >= 0; x--) {

            long currentAddress = addresses.get(x);

            LogData ld = log.read(currentAddress);
            byte[] data = ld.getData();
            ByteBuffer buf = ByteBuffer.wrap(data);

            processEntry(currentAddress, buf);

        }
    }

    void processEntry(long address, ByteBuffer buf) {

        byte type = buf.get();

        if (type != 7) {
            // multi smr object
            throw new IllegalStateException("Unknown entry type " + type);
        }

        int numMultiObjs = buf.getInt();

        for (int x = 0; x < numMultiObjs; x++) {

            UUID id = new UUID(buf.getLong(), buf.getLong());

            // probably need to process this backwards ?
            List<SMRMeta> smrs = parseMultiSMR(buf);
            

        }
    }


    List<SMRMeta> parseMultiSMR(ByteBuffer buf) {

        short type = buf.getShort();

        if (type != 8) {
            throw new IllegalStateException("Expectected a multi smr entry, but type mismatch");
        }

        int smrNum = buf.getInt();
        List<SMRMeta> smrs = new ArrayList<>();

        for (int x = 0; x < smrNum; x++) {
            smrs.add(parseSMR(buf));
        }

        return smrs;
    }

    SMRMeta parseSMR(ByteBuffer buf) {
        int p1 = buf.position();

        // get type
        short type = buf.getShort();

        if (type != 0) {
            throw new IllegalStateException("Expectected an smr entry, but type mismatch");
        }

        // method len and method string
        short methodLen = buf.getShort();
        buf.position(buf.position() + methodLen);

        // serializer type
        buf.get();

        int numArgs = buf.get();

        // get the conflict key
        byte[] conflictKeyBytes = new byte[buf.getInt()];

        buf.get(conflictKeyBytes);

        // Skip rest of bytes
        for (int x = 1; x < numArgs; x++) {
            int argLen = buf.getInt();
            buf.position(buf.position() + argLen);
        }

        int p2 = buf.position();

        return new SMRMeta(ByteBuffer.wrap(conflictKeyBytes), (p2 - p1));
    }

    @Data
    class SMRMeta {
        final ByteBuffer buf;
        final int len;
    }
}
