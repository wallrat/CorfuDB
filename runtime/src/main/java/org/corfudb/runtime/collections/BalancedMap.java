package org.corfudb.runtime.collections;

import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.view.ObjectOpenOptions;

import java.util.Collection;
import java.util.Map;
import java.util.Random;
import java.util.Set;

/**
 * Created by box on 12/8/17.
 */
public class BalancedMap<K, V> implements ISMRMap<K, V> {

    final SMRMap<K, V>[] instances;
    final SMRMap<K, V>[] instancesR;
    final CorfuRuntime rt;

    public BalancedMap(CorfuRuntime rt, int numInstance, String name) {
        this.rt = rt;
        instances = new SMRMap[numInstance];
        instancesR = new SMRMap[numInstance];

        for (int x = 0; x < instances.length; x++) {
            instances[x] = rt.getObjectsView().build()
                    .setStreamName(name)
                    .setType(SMRMap.class)
                    .addOption(ObjectOpenOptions.NO_CACHE)
                    .open();
            instancesR[x] = rt.getObjectsView().build()
                    .setStreamName(name)
                    .setType(SMRMap.class)
                    .addOption(ObjectOpenOptions.NO_CACHE)
                    .open();
        }
    }

    SMRMap<K, V> getMap() {
        int rnd = new Random().nextInt(instances.length);
        return instances[rnd];
    }

    SMRMap<K, V> getMapR() {
        int rnd = new Random().nextInt(instances.length);
        return instancesR[rnd];
    }

    @Override
    public int size() {
        return getMap().size();
    }

    @Override
    public boolean isEmpty() {
        return getMap().isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        return getMap().containsKey(key);
    }

    @Override
    public boolean containsValue(Object value) {
        return getMap().containsValue(value);
    }

    @Override
    public V get(Object key) {
        return getMapR().get(key);
    }

    @Override
    public V put(K key, V value) {
        return getMap().put(key, value);
    }

    @Override
    public void blindPut(K key, V value) {
        getMap().blindPut(key, value);
    }

    @Override
    public V remove(Object key) {
        return getMap().remove(key);
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        getMap().putAll(m);
    }

    @Override
    public void clear() {
        getMap().clear();
    }

    @Override
    public Set<K> keySet() {
        return getMap().keySet();
    }

    @Override
    public Collection<V> values() {
        return getMap().values();
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        return getMap().entrySet();
    }
}
