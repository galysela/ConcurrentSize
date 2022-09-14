package measurements.adapters;

import algorithms.iterator.IteratorSkipList;
import measurements.support.SetInterface;
import measurements.support.ThreadID;

public class IteratorSkipListAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    IteratorSkipList<K, K> set = new IteratorSkipList<K,K>();

    @Override
    public boolean contains(K key) {
        return set.containsKey(key, ThreadID.threadID.get());
    }

    @Override
    public boolean insert(K key) {
        return set.putIfAbsent(key, key, ThreadID.threadID.get()) == null;
    }

    @Override
    public boolean remove(K key) {
        return set.remove(key, ThreadID.threadID.get()) != null;
    }

    @Override
    public int size() {
        return set.iterSize(ThreadID.threadID.get());
    }

    @Override
    public long getKeysum() {
        return set.getSumOfKeys();
    }
}
