package org.corfudb.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import org.corfudb.runtime.collections.CorfuTable;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.openjdk.jmh.annotations.Level.Invocation;
import static org.openjdk.jmh.annotations.Level.Trial;

public class CorfuTableBenchmark {

    @Data
    public static class Key {
        @NonNull
        UUID left;

        @NonNull
        UUID right;

        public Key(UUID left, UUID right) {
            this.left = left;
            this.right = right;
        }
    }

    public enum CompositeKeyIndexer implements CorfuTable.IndexSpecification<Key, Key, UUID, Key> {
        BY_LEFT_OR_RIGHT((k,v) -> ImmutableList.of(k.left, k.right))
        ;

        CompositeKeyIndexer(CorfuTable.IndexFunction<Key, Key, UUID> indexFunction) {
            this.indexFunction = indexFunction;
        }

        final CorfuTable.IndexFunction<Key, Key, UUID> indexFunction;

        @Override
        public CorfuTable.IndexFunction<Key, Key, UUID> getIndexFunction() {
            return indexFunction;
        }

        @Override
        public CorfuTable.ProjectionFunction<Key, Key, UUID, Key> getProjectionFunction() {
            return projectionFunction;
        }

        final CorfuTable.ProjectionFunction<Key, Key, UUID, Key> projectionFunction
                = (i, s) -> s.map(entry -> entry.getValue());
    }

    @State(Scope.Benchmark)
    public static class CorfuTableCommon extends Common {

        CorfuTable<Key, Key, CompositeKeyIndexer, UUID> table;

        Set<Key> keys;

        @Override
        public void setup() {
            super.setup();
            table = rt.getObjectsView()
                    .build()
                    .setTypeToken(
                            new TypeToken<CorfuTable<Key, Key, CompositeKeyIndexer, UUID>>() {})
                    .setArguments(CompositeKeyIndexer.class)
                    .setStreamName(Integer.toString(RND.nextInt()))
                    .open();

            keys = new HashSet<>();
        }

        void write(int batch) {
            for (int x = 0; x < num; x++) {
                UUID left = UUID.randomUUID();
                UUID right = UUID.randomUUID();
                Key key = new Key(left, right);
                table.put(key, key);
                keys.add(key);
            }
        }

        @Override
        public void teardown() {
            super.teardown();
            keys.clear();
        }
    }

    @State(Scope.Benchmark)
    public static class Writer extends CorfuTableCommon {

        @Setup(Invocation)
        @Override
        public void setup() {
            super.setup();
        }

        @TearDown(Invocation)
        @Override
        public void teardown() {
            super.teardown();
        }
    }

    @State(Scope.Benchmark)
    public static class Reader extends CorfuTableCommon {

        @Setup(Trial)
        @Override
        public void setup() {
            super.setup();
            super.write(num);
        }

        @TearDown(Trial)
        @Override
        public void teardown() {
            super.teardown();
        }
    }

    @Benchmark
    public void tableInsert(Writer w, Blackhole bh) {
        w.rt.getObjectsView().TXBegin();
        w.write(w.num);
        w.rt.getObjectsView().TXEnd();
        bh.consume(w.num);
    }

    @Benchmark
    public void tableGet(Reader r, Blackhole bh) {
        r.rt.getObjectsView().TXBegin();
        for (Key key : r.keys) {
            Key k = r.table.get(key);
            bh.consume(k);
        }
        r.rt.getObjectsView().TXEnd();
    }

    @Benchmark
    public void tableSecondaryIndexGet(Reader r, Blackhole bh) {
        r.rt.getObjectsView().TXBegin();

        for (Key key : r.keys) {
            Collection<Object> ret = r.table.getByIndex(CompositeKeyIndexer.BY_LEFT_OR_RIGHT, key.right);
            bh.consume(ret.size());
        }

        r.rt.getObjectsView().TXEnd();
    }
}
