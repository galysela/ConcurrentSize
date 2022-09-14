package measurements.adapters;

import algorithms.baseline.HashTable;
import measurements.support.SetInterface;

public class HashTableAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    HashTable<K,K> set;

    public HashTableAdapter(int tableSize) {
        set = new HashTable<K,K>(tableSize);
    }

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
