
package measurements.adapters;

import algorithms.size.SizeBST;
import measurements.support.SetInterface;

public class SizeBSTAdapter<K extends Comparable<? super K>> extends AbstractAdapter<K> implements SetInterface<K> {
    SizeBST<K,K> tree = new SizeBST<K,K>();

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
    public int size() {
        return tree.size();
    }

    @Override
    public long getKeysum() {
        return tree.getSumOfKeys();
    }
}
