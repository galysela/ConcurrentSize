package measurements.adapters;

import algorithms.size.SizeConcurrentSkipListMap;
import measurements.support.SetInterface;

public class SizeSkipListAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    SizeConcurrentSkipListMap<K,K> set = new SizeConcurrentSkipListMap<K,K>();

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
    public int size() {
        return set.size();
    }

    @Override
    public long getKeysum() {
        return set.getSumOfKeys();
    }
}
