package measurements.adapters;

import algorithms.baseline.BST;
import measurements.support.SetInterface;

public class BSTAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    BST<K,K> tree = new BST<K,K>();

    @Override
    public boolean contains(K key) {
        return tree.containsKey(key);
    }

    @Override
    public boolean insert(K key) {
        return tree.putIfAbsent(key, key) == null;
    }

    @Override
    public boolean remove(K key) {
        return tree.remove(key) != null;
    }

    @Override
    public long getKeysum() {
        return tree.getSumOfKeys();
    }
}
