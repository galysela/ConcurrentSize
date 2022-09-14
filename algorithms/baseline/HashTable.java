package algorithms.baseline;

/**
 *  This file implements a hash table as a table of linked lists, whose implementation is based on the
 *  linked list in the base level of java.util.concurrent.ConcurrentSkipListMap by Doug Lea.
 */

/*
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

/*
 * This file is available under and governed by the GNU General Public
 * License version 2 only, as published by the Free Software Foundation.
 * However, the following notice accompanied the original version of this
 * file:
 *
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Comparator;

public class HashTable<K,V> {
    /**
     * The comparator used to maintain order in this map, or null if
     * using natural ordering.  (Non-private to simplify access in
     * nested classes.)
     */
    final Comparator<? super K> comparator;

    private final int tableSize;
    private final Node<K,V>[] table;

    /* ------ Taken from https://github.com/openjdk/jdk/blob/dc7d30d08eacbe4d00d16b13e921359d38c77cd8/src/java.base/share/classes/java/util/concurrent/ConcurrentHashMap.java ------ */

    private static final int MAXIMUM_CAPACITY = 1 << 30;
    static final int HASH_BITS = 0x7fffffff;

    /**
     * Spreads (XORs) higher bits of hash to lower and also forces top
     * bit to 0. Because the table uses power-of-two masking, sets of
     * hashes that vary only in bits above the current mask will
     * always collide. (Among known examples are sets of Float keys
     * holding consecutive whole numbers in small tables.)  So we
     * apply a transform that spreads the impact of higher bits
     * downward. There is a tradeoff between speed, utility, and
     * quality of bit-spreading. Because many common sets of hashes
     * are already reasonably distributed (so don't benefit from
     * spreading), and because we use trees to handle large sets of
     * collisions in bins, we just XOR some shifted bits in the
     * cheapest possible way to reduce systematic lossage, as well as
     * to incorporate impact of the highest bits that would otherwise
     * never be used in index calculations because of table bounds.
     */
    static final int spread(int h) {
        return (h ^ (h >>> 16)) & HASH_BITS;
    }

    /**
     * Returns a power of two table size for the given desired capacity.
     * See Hackers Delight, sec 3.2
     */
    private static final int tableSizeFor(int c) {
        int n = -1 >>> Integer.numberOfLeadingZeros(c - 1);
        return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
    }

    /* ---------------- Node -------------- */

    /**
     * Nodes hold keys and values, and are singly linked in sorted
     * order, possibly with some intervening marker nodes. The list is
     * headed by a header node accessible as head.node. Headers and
     * marker nodes have null keys. The val field (but currently not
     * the key field) is nulled out upon deletion.
     */
    static final class Node<K,V> {
        final K key;
        V val;
        Node<K,V> next;

        Node(K key, V value, Node<K,V> next) {
            this.key = key;
            this.val = value;
            this.next = next;
        }
    }

    /* ---------------- Constructors -------------- */

    /**
     * Constructs a new, empty map, sorted according to the
     * {@linkplain Comparable natural ordering} of the keys.
     */
    public HashTable(int requestedTableSize) {
        this(requestedTableSize, null);
    }

    /**
     * Constructs a new, empty map, sorted according to the specified
     * comparator.
     *
     * @param comparator the comparator that will be used to order this map.
     *        If {@code null}, the {@linkplain Comparable natural
     *        ordering} of the keys will be used.
     */
    public HashTable(int requestedTableSize, Comparator<? super K> comparator) {
        this.comparator = comparator;

        if (requestedTableSize <= 0) throw new NegativeArraySizeException();
        this.tableSize = tableSizeFor(requestedTableSize);
        Node<K,V>[] constructedTable = new Node[tableSize];
        // Initialize each of the table cells with a dummy head for its linked list:
        for (int i = 0; i < tableSize; ++i) {
            constructedTable[i] = new Node<K, V>(null, null, null);
        }
        this.table = constructedTable;
        // Now the table's content is visible to all, see https://stackoverflow.com/questions/2830739/do-the-up-to-date-guarantees-for-values-of-javas-final-fields-extend-to-indir
    }

    private Node<K,V> getListHead(Object key) {
        int keyHash = spread(key.hashCode());
        int tableIndex = (tableSize - 1) & keyHash;
        return table[tableIndex];
    }

    private V doGet(Object key) {
        return listDoGet(key, getListHead(key));
    }

    private V doPut(K key, V value, boolean onlyIfAbsent) {
        return listDoPut(key, value, onlyIfAbsent, getListHead(key));
    }

    private V doRemove(Object key, Object value) {
        return listDoRemove(key, value, getListHead(key));
    }

    /* ---------------- List traversal -------------- */

    /**
     * Gets value for key. Same idea as findNode, except skips over
     * deletions and markers, and returns first encountered value to
     * avoid possibly inconsistent rereads.
     *
     * @param key the key
     * @return the value, or null if absent
     */
    private V listDoGet(Object key, Node<K,V> head) {
        VarHandle.acquireFence();
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        V result = null;
        Node<K,V> b = head;
        Node<K,V> n;
        while ((n = b.next) != null) {
            V v; int c;
            K k = n.key;
            if ((v = n.val) == null || k == null ||
                    (c = cpr(cmp, key, k)) > 0)
                b = n;
            else {
                if (c == 0)
                    result = v; // possibly null, if n is removed
                break;
            }
        }

        return result;
    }

    /* ---------------- List insertion -------------- */

    /**
     * Main insertion method.  Adds element if not present, or
     * replaces value if present and onlyIfAbsent is false.
     *
     * @param key the key
     * @param value the value that must be associated with key
     * @param onlyIfAbsent if should not insert if already present
     * @return the old value, or null if newly inserted
     */
    private V listDoPut(K key, V value, boolean onlyIfAbsent, Node<K,V> head) {
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        for (;;) {
            VarHandle.acquireFence();
            Node<K,V> b = head;
            for (;;) {                       // find insertion point
                Node<K,V> n; K k; V v; int c;
                if ((n = b.next) == null) {
                    if (b.key == null)       // if empty, type check key now
                        cpr(cmp, key, key);
                    c = -1;
                }
                else if ((k = n.key) == null)
                    break;                   // can't append; restart
                else if ((v = n.val) == null) {
                    completeRemove(b, n);
                    c = 1;
                }
                else if ((c = cpr(cmp, key, k)) > 0)
                    b = n;
                else if (c == 0 &&
                        (onlyIfAbsent || VAL.compareAndSet(n, v, value)))
                    return v;

                if (c < 0 &&
                        NEXT.compareAndSet(b, n, new Node<K,V>(key, value, n)))
                    return null;
            }
        }
    }

    /* ---------------- List deletion -------------- */

    /**
     * Main deletion method. Locates node, nulls value, appends a
     * deletion marker and unlinks predecessor.
     *
     * @param key the key
     * @param value if non-null, the value that must be
     * associated with key
     * @return the node, or null if not found
     */
    final V listDoRemove(Object key, Object value, Node<K,V> head) {
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        V result = null;
        outer: for (;;) { // Each iteration starts a traversal from the head
            VarHandle.acquireFence();
            Node<K,V> b = head;
            for (;;) { // Each iteration advances the pointers one step ahead in the list
                Node<K,V> n; K k; V v; int c;
                if ((n = b.next) == null)
                    break outer;
                else if ((k = n.key) == null)
                    break;
                else if ((v = n.val) == null)
                    completeRemove(b, n);
                else if ((c = cpr(cmp, key, k)) > 0)
                    b = n;
                else if (c < 0)
                    break outer;
                else if (value != null && !value.equals(v))
                    break outer;
                else if (VAL.compareAndSet(n, v, null)) {
                    result = v;
                    completeRemove(b, n);
                    break outer;
                }
            }
        }
        return result;
    }

    /* ----------------  Utilities -------------- */

    /**
     * Compares using comparator or natural ordering if null.
     * Called only by methods that have performed required type checks.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    static int cpr(Comparator c, Object x, Object y) {
        return (c != null) ? c.compare(x, y) : ((Comparable)x).compareTo(y);
    }

    /**
     * Tries to unlink deleted node n from predecessor b (if both
     * exist), by first splicing in a marker if not already present.
     * Upon return, node n is sure to be unlinked from b, possibly
     * via the actions of some other thread.
     *
     * 	1. Insert marker node succeeding n
     *  2. Unlink n
     *
     * @param b if nonnull, predecessor
     * @param n if nonnull, node known to be deleted
     */
    static <K,V> void completeRemove(Node<K,V> b, Node<K,V> n) {
        if (b != null && n != null) {
            Node<K,V> f, p;
            for (;;) {
                if ((f = n.next) != null && f.key == null) {
                    p = f.next;               // already marked
                    break;
                }
                else if (NEXT.compareAndSet(n, f,
                        new Node<K,V>(null, null, f))) {
                    p = f;                    // add marker
                    break;
                }
            }
            NEXT.compareAndSet(b, n, p);
        }
    }

    /* ------ Map API methods ------ */

    /**
     * Returns {@code true} if this map contains a mapping for the specified
     * key.
     *
     * @param key key whose presence in this map is to be tested
     * @return {@code true} if this map contains a mapping for the specified key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public boolean containsKey(Object key) {
        return doGet(key) != null;
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code key} compares
     * equal to {@code k} according to the map's ordering, then this
     * method returns {@code v}; otherwise it returns {@code null}.
     * (There can be at most one such mapping.)
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public V get(Object key) {
        return doGet(key);
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or the given defaultValue if this map contains no mapping for the key.
     *
     * @param key the key
     * @param defaultValue the value to return if this map contains
     * no mapping for the given key
     * @return the mapping for the key, if present; else the defaultValue
     * @throws NullPointerException if the specified key is null
     * @since 1.8
     */
    public V getOrDefault(Object key, V defaultValue) {
        V v;
        return (v = doGet(key)) == null ? defaultValue : v;
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for the key, the old
     * value is replaced.
     *
     * @param key key with which the specified value is to be associated
     * @param value value to be associated with the specified key
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    public V put(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        return doPut(key, value, false);
    }

    /**
     * Removes the mapping for the specified key from this map if present.
     *
     * @param  key key for which mapping should be removed
     * @return the previous value associated with the specified key, or
     *         {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public V remove(Object key) {
        return doRemove(key, null);
    }

    /* ------ ConcurrentMap API methods ------ */

    /**
     * {@inheritDoc}
     *
     * @return the previous value associated with the specified key,
     *         or {@code null} if there was no mapping for the key
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key or value is null
     */
    public V putIfAbsent(K key, V value) {
        if (value == null)
            throw new NullPointerException();
        return doPut(key, value, true);
    }

    /**
     * {@inheritDoc}
     *
     * @throws ClassCastException if the specified key cannot be compared
     *         with the keys currently in the map
     * @throws NullPointerException if the specified key is null
     */
    public boolean remove(Object key, Object value) {
        if (key == null)
            throw new NullPointerException();
        return value != null && doRemove(key, value) != null;
    }

    /* ------ SortedMap API methods ------ */

    public Comparator<? super K> comparator() {
        return comparator;
    }

    // VarHandle mechanics
    private static final VarHandle NEXT;
    private static final VarHandle VAL;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            NEXT = l.findVarHandle(Node.class, "next", Node.class);
            VAL = l.findVarHandle(Node.class, "val", Object.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // For debug
    public long getSumOfKeys() {
        Node<K, V> n;
        long keysSum = 0;
        for (int i = 0; i < tableSize; i++) {
            Node<K, V> b = table[i];
            while ((n = b.next) != null) {
                K k = n.key;
                if (n.val != null && k != null)
                    keysSum += (Integer) k;
                b = n;
            }
        }
        return keysSum;
    }
}