package measurements.adapters;

import algorithms.baseline.ConcurrentSkipListMap;
import measurements.support.SetInterface;

public class SkipListAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    ConcurrentSkipListMap<K,K> set = new ConcurrentSkipListMap<K,K>();

    @Override
    public boolean contains(K key) {
        return set.containsKey(key);
    }

    @Override
    public boolean insert(K key) {
        return set.putIfAbsent(key, key) == null;
    }

    @Override
    public boolean remove(K key) {
        return set.remove(key) != null;
    }

    @Override
    public long getKeysum() {
        return set.getSumOfKeys();
    }
}
