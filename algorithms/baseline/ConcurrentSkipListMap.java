package algorithms.baseline;

/**
 *  This file is a variant of https://github.com/openjdk/jdk/blob/739769c8fc4b496f08a92225a12d07414537b6c0/src/java.base/share/classes/java/util/concurrent/ConcurrentSkipListMap.java
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
import java.util.concurrent.ThreadLocalRandom;

/**
 * A scalable concurrent skiplist map implementation.
 * The map is sorted according to the {@linkplain Comparable natural
 * ordering} of its keys, or by a {@link Comparator} provided at map
 * creation time, depending on which constructor is used.
 *
 * <p>This class implements a concurrent variant of <a
 * href="http://en.wikipedia.org/wiki/Skip_list" target="_top">SkipLists</a>
 * providing expected average <i>log(n)</i> time cost for the
 * {@code containsKey}, {@code get}, {@code put} and
 * {@code remove} operations and their variants.  Insertion, removal,
 * update, and access operations safely execute concurrently by
 * multiple threads.
 *
 * <p>This class is a member of the
 * <a href="{@docRoot}/java.base/java/util/package-summary.html#CollectionsFramework">
 * Java Collections Framework</a>.
 *
 * @author Doug Lea
 * @param <K> the type of keys maintained by this map
 * @param <V> the type of mapped values
 * @since 1.6
 */
public class ConcurrentSkipListMap<K,V> {
    /*
     * This class implements a tree-like two-dimensionally linked skip
     * list in which the index levels are represented in separate
     * nodes from the base nodes holding data.  There are two reasons
     * for taking this approach instead of the usual array-based
     * structure: 1) Array based implementations seem to encounter
     * more complexity and overhead 2) We can use cheaper algorithms
     * for the heavily-traversed index lists than can be used for the
     * base lists.  Here's a picture of some of the basics for a
     * possible list with 2 levels of index:
     *
     * Head nodes          Index nodes
     * +-+    right        +-+                      +-+
     * |2|---------------->| |--------------------->| |->null
     * +-+                 +-+                      +-+
     *  | down              |                        |
     *  v                   v                        v
     * +-+            +-+  +-+       +-+            +-+       +-+
     * |1|----------->| |->| |------>| |----------->| |------>| |->null
     * +-+            +-+  +-+       +-+            +-+       +-+
     *  v              |    |         |              |         |
     * Nodes  next     v    v         v              v         v
     * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
     * | |->|A|->|B|->|C|->|D|->|E|->|F|->|G|->|H|->|I|->|J|->|K|->null
     * +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+  +-+
     *
     * The base lists use a variant of the HM linked ordered set
     * algorithm. See Tim Harris, "A pragmatic implementation of
     * non-blocking linked lists"
     * http://www.cl.cam.ac.uk/~tlh20/publications.html and Maged
     * Michael "High Performance Dynamic Lock-Free Hash Tables and
     * List-Based Sets"
     * http://www.research.ibm.com/people/m/michael/pubs.htm.  The
     * basic idea in these lists is to mark the "next" pointers of
     * deleted nodes when deleting to avoid conflicts with concurrent
     * insertions, and when traversing to keep track of triples
     * (predecessor, node, successor) in order to detect when and how
     * to unlink these deleted nodes.
     *
     * Rather than using mark-bits to mark list deletions (which can
     * be slow and space-intensive using AtomicMarkedReference), nodes
     * use direct CAS'able next pointers.  On deletion, instead of
     * marking a pointer, they splice in another node that can be
     * thought of as standing for a marked pointer (see method
     * unlinkNode).  Using plain nodes acts roughly like "boxed"
     * implementations of marked pointers, but uses new nodes only
     * when nodes are deleted, not for every link.  This requires less
     * space and supports faster traversal. Even if marked references
     * were better supported by JVMs, traversal using this technique
     * might still be faster because any search need only read ahead
     * one more node than otherwise required (to check for trailing
     * marker) rather than unmasking mark bits or whatever on each
     * read.
     *
     * This approach maintains the essential property needed in the HM
     * algorithm of changing the next-pointer of a deleted node so
     * that any other CAS of it will fail, but implements the idea by
     * changing the pointer to point to a different node (with
     * otherwise illegal null fields), not by marking it.  While it
     * would be possible to further squeeze space by defining marker
     * nodes not to have key/value fields, it isn't worth the extra
     * type-testing overhead.  The deletion markers are rarely
     * encountered during traversal, are easily detected via null
     * checks that are needed anyway, and are normally quickly garbage
     * collected. (Note that this technique would not work well in
     * systems without garbage collection.)
     *
     * In addition to using deletion markers, the lists also use
     * nullness of value fields to indicate deletion, in a style
     * similar to typical lazy-deletion schemes.  If a node's value is
     * null, then it is considered logically deleted and ignored even
     * though it is still reachable.
     *
     * Here's the sequence of events for a deletion of node n with
     * predecessor b and successor f, initially:
     *
     *        +------+       +------+      +------+
     *   ...  |   b  |------>|   n  |----->|   f  | ...
     *        +------+       +------+      +------+
     *
     * 1. CAS n's value field from non-null to null.
     *    Traversals encountering a node with null value ignore it.
     *    However, ongoing insertions and deletions might still modify
     *    n's next pointer.
     *
     * 2. CAS n's next pointer to point to a new marker node.
     *    From this point on, no other nodes can be appended to n.
     *    which avoids deletion errors in CAS-based linked lists.
     *
     *        +------+       +------+      +------+       +------+
     *   ...  |   b  |------>|   n  |----->|marker|------>|   f  | ...
     *        +------+       +------+      +------+       +------+
     *
     * 3. CAS b's next pointer over both n and its marker.
     *    From this point on, no new traversals will encounter n,
     *    and it can eventually be GCed.
     *        +------+                                    +------+
     *   ...  |   b  |----------------------------------->|   f  | ...
     *        +------+                                    +------+
     *
     * A failure at step 1 leads to simple retry due to a lost race
     * with another operation. Steps 2-3 can fail because some other
     * thread noticed during a traversal a node with null value and
     * helped out by marking and/or unlinking.  This helping-out
     * ensures that no thread can become stuck waiting for progress of
     * the deleting thread.
     *
     * Skip lists add indexing to this scheme, so that the base-level
     * traversals start close to the locations being found, inserted
     * or deleted -- usually base level traversals only traverse a few
     * nodes. This doesn't change the basic algorithm except for the
     * need to make sure base traversals start at predecessors (here,
     * b) that are not (structurally) deleted, otherwise retrying
     * after processing the deletion.
     *
     * Index levels are maintained using CAS to link and unlink
     * successors ("right" fields).  Races are allowed in index-list
     * operations that can (rarely) fail to link in a new index node.
     * (We can't do this of course for data nodes.)  However, even
     * when this happens, the index lists correctly guide search.
     * This can impact performance, but since skip lists are
     * probabilistic anyway, the net result is that under contention,
     * the effective "p" value may be lower than its nominal value.
     *
     * Index insertion and deletion sometimes require a separate
     * traversal pass occurring after the base-level action, to add or
     * remove index nodes.  This adds to single-threaded overhead, but
     * improves contended multithreaded performance by narrowing
     * interference windows, and allows deletion to ensure that all
     * index nodes will be made unreachable upon return from a public
     * remove operation, thus avoiding unwanted garbage retention.
     *
     * Indexing uses skip list parameters that maintain good search
     * performance while using sparser-than-usual indices: The
     * hardwired parameters k=1, p=0.5 (see method doPut) mean that
     * about one-quarter of the nodes have indices. Of those that do,
     * half have one level, a quarter have two, and so on (see Pugh's
     * Skip List Cookbook, sec 3.4), up to a maximum of 62 levels
     * (appropriate for up to 2^63 elements).  The expected total
     * space requirement for a map is slightly less than for the
     * current implementation of java.util.TreeMap.
     *
     * Changing the level of the index (i.e, the height of the
     * tree-like structure) also uses CAS.  Creation of an index with
     * height greater than the current level adds a level to the head
     * index by CAS'ing on a new top-most head. To maintain good
     * performance after a lot of removals, deletion methods
     * heuristically try to reduce the height if the topmost levels
     * appear to be empty.  This may encounter races in which it is
     * possible (but rare) to reduce and "lose" a level just as it is
     * about to contain an index (that will then never be
     * encountered). This does no structural harm, and in practice
     * appears to be a better option than allowing unrestrained growth
     * of levels.
     *
     * This class provides concurrent-reader-style memory consistency,
     * ensuring that read-only methods report status and/or values no
     * staler than those holding at method entry. This is done by
     * performing all publication and structural updates using
     * (volatile) CAS, placing an acquireFence in a few access
     * methods, and ensuring that linked objects are transitively
     * acquired via dependent reads (normally once) unless performing
     * a volatile-mode CAS operation (that also acts as an acquire and
     * release).  This form of fence-hoisting is similar to RCU and
     * related techniques (see McKenney's online book
     * https://www.kernel.org/pub/linux/kernel/people/paulmck/perfbook/perfbook.html)
     * It minimizes overhead that may otherwise occur when using so
     * many volatile-mode reads. Using explicit acquireFences is
     * logistically easier than targeting particular fields to be read
     * in acquire mode: fences are just hoisted up as far as possible,
     * to the entry points or loop headers of a few methods. A
     * potential disadvantage is that these few remaining fences are
     * not easily optimized away by compilers under exclusively
     * single-thread use.  It requires some care to avoid volatile
     * mode reads of other fields. (Note that the memory semantics of
     * a reference dependently read in plain mode exactly once are
     * equivalent to those for atomic opaque mode.)  Iterators and
     * other traversals encounter each node and value exactly once.
     * Other operations locate an element (or position to insert an
     * element) via a sequence of dereferences. This search is broken
     * into two parts. Method findPredecessor (and its specialized
     * embeddings) searches index nodes only, returning a base-level
     * predecessor of the key. Callers carry out the base-level
     * search, restarting if encountering a marker preventing link
     * modification.  In some cases, it is possible to encounter a
     * node multiple times while descending levels. For mutative
     * operations, the reported value is validated using CAS (else
     * retrying), preserving linearizability with respect to each
     * other. Others may return any (non-null) value holding in the
     * course of the method call.  (Search-based methods also include
     * some useless-looking explicit null checks designed to allow
     * more fields to be nulled out upon removal, to reduce floating
     * garbage, but which is not currently done, pending discovery of
     * a way to do this with less impact on other operations.)
     *
     * To produce random values without interference across threads,
     * we use within-JDK thread local random support.
     *
     * For explanation of algorithms sharing at least a couple of
     * features with this one, see Mikhail Fomitchev's thesis
     * (http://www.cs.yorku.ca/~mikhail/), Keir Fraser's thesis
     * (http://www.cl.cam.ac.uk/users/kaf24/), and Hakan Sundell's
     * thesis (http://www.cs.chalmers.se/~phs/).
     *
     * Notation guide for local variables
     * Node:         b, n, f, p for  predecessor, node, successor, aux
     * Index:        q, r, d    for index node, right, down.
     * Head:         h
     * Keys:         k, key
     * Values:       v, value
     * Comparisons:  c
     */

    /**
     * The comparator used to maintain order in this map, or null if
     * using natural ordering.  (Non-private to simplify access in
     * nested classes.)
     */
    final Comparator<? super K> comparator;

    /** Lazily initialized topmost index of the skiplist. */
    private transient Index<K,V> head;

    /**
     * Nodes hold keys and values, and are singly linked in sorted
     * order, possibly with some intervening marker nodes. The list is
     * headed by a header node accessible as head.node. Headers and
     * marker nodes have null keys. The val field (but currently not
     * the key field) is nulled out upon deletion.
     */
    static final class Node<K,V> {
        final K key; // currently, never detached
        V val;
        Node<K,V> next;
        Node(K key, V value, Node<K,V> next) {
            this.key = key;
            this.val = value;
            this.next = next;
        }
    }

    /**
     * Index nodes represent the levels of the skip list.
     */
    static final class Index<K,V> {
        final Node<K,V> node;  // currently, never detached
        final Index<K,V> down;
        Index<K,V> right;
        Index(Node<K,V> node, Index<K,V> down, Index<K,V> right) {
            this.node = node;
            this.down = down;
            this.right = right;
        }
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
     * Returns the header for base node list, or null if uninitialized
     */
    final Node<K,V> baseHead() {
        Index<K,V> h;
        VarHandle.acquireFence();
        return ((h = head) == null) ? null : h.node;
    }

    /**
     * Tries to unlink deleted node n from predecessor b (if both
     * exist), by first splicing in a marker if not already present.
     * Upon return, node n is sure to be unlinked from b, possibly
     * via the actions of some other thread.
     *
     * @param b if nonnull, predecessor
     * @param n if nonnull, node known to be deleted
     */
    static <K,V> void unlinkNode(Node<K,V> b, Node<K,V> n) {
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

    /* ---------------- Traversal -------------- */

    /**
     * Returns an index node with key strictly less than given key.
     * Also unlinks indexes to deleted nodes found along the way.
     * Callers rely on this side-effect of clearing indices to deleted
     * nodes.
     *
     * @param key if nonnull the key
     * @return a predecessor node of key, or null if uninitialized or null key
     */
    private Node<K,V> findPredecessor(Object key, Comparator<? super K> cmp) {
        Index<K,V> q;
        VarHandle.acquireFence();
        if ((q = head) == null || key == null)
            return null;
        else {
            for (Index<K,V> r, d;;) {
                while ((r = q.right) != null) {
                    Node<K,V> p; K k;
                    if ((p = r.node) == null || (k = p.key) == null ||
                            p.val == null)  // unlink index to deleted node
                        RIGHT.compareAndSet(q, r, r.right);
                    else if (cpr(cmp, key, k) > 0)
                        q = r;
                    else
                        break;
                }
                if ((d = q.down) != null)
                    q = d;
                else
                    return q.node;
            }
        }
    }

    /**
     * Gets value for key. Same idea as findNode, except skips over
     * deletions and markers, and returns first encountered value to
     * avoid possibly inconsistent rereads.
     *
     * @param key the key
     * @return the value, or null if absent
     */
    private V doGet(Object key) {
        Index<K,V> q;
        VarHandle.acquireFence();
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        V result = null;
        if ((q = head) != null) {
            outer: for (Index<K,V> r, d;;) {
                while ((r = q.right) != null) {
                    Node<K,V> p; K k; V v; int c;
                    if ((p = r.node) == null || (k = p.key) == null ||
                            (v = p.val) == null)
                        RIGHT.compareAndSet(q, r, r.right);
                    else if ((c = cpr(cmp, key, k)) > 0)
                        q = r;
                    else if (c == 0) {
                        result = v;
                        break outer;
                    }
                    else
                        break;
                }
                if ((d = q.down) != null)
                    q = d;
                else {
                    Node<K,V> b, n;
                    if ((b = q.node) != null) {
                        while ((n = b.next) != null) {
                            V v; int c;
                            K k = n.key;
                            if ((v = n.val) == null || k == null ||
                                    (c = cpr(cmp, key, k)) > 0)
                                b = n;
                            else {
                                if (c == 0)
                                    result = v;
                                break;
                            }
                        }
                    }
                    break;
                }
            }
        }
        return result;
    }

    /* ---------------- Insertion -------------- */

    /**
     * Main insertion method.  Adds element if not present, or
     * replaces value if present and onlyIfAbsent is false.
     *
     * @param key the key
     * @param value the value that must be associated with key
     * @param onlyIfAbsent if should not insert if already present
     * @return the old value, or null if newly inserted
     */
    private V doPut(K key, V value, boolean onlyIfAbsent) {
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        for (;;) {
            Index<K,V> h; Node<K,V> b;
            VarHandle.acquireFence();
            int levels = 0;                    // number of levels descended
            if ((h = head) == null) {          // try to initialize
                Node<K,V> base = new Node<K,V>(null, null, null);
                h = new Index<K,V>(base, null, null);
                b = (HEAD.compareAndSet(this, null, h)) ? base : null;
            }
            else {
                for (Index<K,V> q = h, r, d;;) { // count while descending
                    while ((r = q.right) != null) {
                        Node<K,V> p; K k;
                        if ((p = r.node) == null || (k = p.key) == null ||
                                p.val == null)
                            RIGHT.compareAndSet(q, r, r.right);
                        else if (cpr(cmp, key, k) > 0)
                            q = r;
                        else
                            break;
                    }
                    if ((d = q.down) != null) {
                        ++levels;
                        q = d;
                    }
                    else {
                        b = q.node;
                        break;
                    }
                }
            }
            if (b != null) {
                Node<K,V> z = null;              // new node, if inserted
                for (;;) {                       // find insertion point
                    Node<K,V> n, p; K k; V v; int c;
                    if ((n = b.next) == null) {
                        if (b.key == null)       // if empty, type check key now
                            cpr(cmp, key, key);
                        c = -1;
                    }
                    else if ((k = n.key) == null)
                        break;                   // can't append; restart
                    else if ((v = n.val) == null) {
                        unlinkNode(b, n);
                        c = 1;
                    }
                    else if ((c = cpr(cmp, key, k)) > 0)
                        b = n;
                    else if (c == 0 &&
                            (onlyIfAbsent || VAL.compareAndSet(n, v, value)))
                        return v;

                    if (c < 0 &&
                            NEXT.compareAndSet(b, n,
                                    p = new Node<K,V>(key, value, n))) {
                        z = p;
                        break;
                    }
                }

                if (z != null) {
                    int lr = ThreadLocalRandom.current().nextInt();
                    if ((lr & 0x3) == 0) {       // add indices with 1/4 prob
                        int hr = ThreadLocalRandom.current().nextInt();
                        long rnd = ((long)hr << 32) | ((long)lr & 0xffffffffL);
                        int skips = levels;      // levels to descend before add
                        Index<K,V> x = null;
                        for (;;) {               // create at most 62 indices
                            x = new Index<K,V>(z, x, null);
                            if (rnd >= 0L || --skips < 0)
                                break;
                            else
                                rnd <<= 1;
                        }
                        if (addIndices(h, skips, x, cmp) && skips < 0 &&
                                head == h) {         // try to add new level
                            Index<K,V> hx = new Index<K,V>(z, x, null);
                            Index<K,V> nh = new Index<K,V>(h.node, h, hx);
                            HEAD.compareAndSet(this, h, nh);
                        }
                        if (z.val == null)       // deleted while adding indices
                            findPredecessor(key, cmp); // clean
                    }
                    return null;
                }
            }
        }
    }

    /**
     * Add indices after an insertion. Descends iteratively to the
     * highest level of insertion, then recursively, to chain index
     * nodes to lower ones. Returns null on (staleness) failure,
     * disabling higher-level insertions. Recursion depths are
     * exponentially less probable.
     *
     * @param q starting index for current level
     * @param skips levels to skip before inserting
     * @param x index for this insertion
     * @param cmp comparator
     */
    static <K,V> boolean addIndices(Index<K,V> q, int skips, Index<K,V> x,
                                    Comparator<? super K> cmp) {
        Node<K,V> z; K key;
        if (x != null && (z = x.node) != null && (key = z.key) != null &&
                q != null) {                            // hoist checks
            boolean retrying = false;
            for (;;) {                              // find splice point
                Index<K,V> r, d; int c;
                if ((r = q.right) != null) {
                    Node<K,V> p; K k;
                    if ((p = r.node) == null || (k = p.key) == null ||
                            p.val == null) {
                        RIGHT.compareAndSet(q, r, r.right);
                        c = 0;
                    }
                    else if ((c = cpr(cmp, key, k)) > 0)
                        q = r;
                    else if (c == 0)
                        break;                      // stale
                }
                else
                    c = -1;

                if (c < 0) {
                    if ((d = q.down) != null && skips > 0) {
                        --skips;
                        q = d;
                    }
                    else if (d != null && !retrying &&
                            !addIndices(d, 0, x.down, cmp))
                        break;
                    else {
                        x.right = r;
                        if (RIGHT.compareAndSet(q, r, x))
                            return true;
                        else
                            retrying = true;         // re-find splice point
                    }
                }
            }
        }
        return false;
    }

    /* ---------------- Deletion -------------- */

    /**
     * Main deletion method. Locates node, nulls value, appends a
     * deletion marker, unlinks predecessor, removes associated index
     * nodes, and possibly reduces head index level.
     *
     * @param key the key
     * @param value if non-null, the value that must be
     * associated with key
     * @return the node, or null if not found
     */
    final V doRemove(Object key, Object value) {
        if (key == null)
            throw new NullPointerException();
        Comparator<? super K> cmp = comparator;
        V result = null;
        Node<K,V> b;
        outer: while ((b = findPredecessor(key, cmp)) != null &&
                result == null) {
            for (;;) {
                Node<K,V> n; K k; V v; int c;
                if ((n = b.next) == null)
                    break outer;
                else if ((k = n.key) == null)
                    break;
                else if ((v = n.val) == null)
                    unlinkNode(b, n);
                else if ((c = cpr(cmp, key, k)) > 0)
                    b = n;
                else if (c < 0)
                    break outer;
                else if (value != null && !value.equals(v))
                    break outer;
                else if (VAL.compareAndSet(n, v, null)) {
                    result = v;
                    unlinkNode(b, n);
                    break; // loop to clean up
                }
            }
        }
        if (result != null) {
            tryReduceLevel();
        }
        return result;
    }

    /**
     * Possibly reduce head level if it has no nodes.  This method can
     * (rarely) make mistakes, in which case levels can disappear even
     * though they are about to contain index nodes. This impacts
     * performance, not correctness.  To minimize mistakes as well as
     * to reduce hysteresis, the level is reduced by one only if the
     * topmost three levels look empty. Also, if the removed level
     * looks non-empty after CAS, we try to change it back quick
     * before anyone notices our mistake! (This trick works pretty
     * well because this method will practically never make mistakes
     * unless current thread stalls immediately before first CAS, in
     * which case it is very unlikely to stall again immediately
     * afterwards, so will recover.)
     *
     * We put up with all this rather than just let levels grow
     * because otherwise, even a small map that has undergone a large
     * number of insertions and removals will have a lot of levels,
     * slowing down access more than would an occasional unwanted
     * reduction.
     */
    private void tryReduceLevel() {
        Index<K,V> h, d, e;
        if ((h = head) != null && h.right == null &&
                (d = h.down) != null && d.right == null &&
                (e = d.down) != null && e.right == null &&
                HEAD.compareAndSet(this, h, d) &&
                h.right != null)   // recheck
            HEAD.compareAndSet(this, d, h);  // try to backout
    }

    /* ---------------- Constructors -------------- */

    /**
     * Constructs a new, empty map, sorted according to the
     * {@linkplain Comparable natural ordering} of the keys.
     */
    public ConcurrentSkipListMap() {
        this.comparator = null;
    }

    /**
     * Constructs a new, empty map, sorted according to the specified
     * comparator.
     *
     * @param comparator the comparator that will be used to order this map.
     *        If {@code null}, the {@linkplain Comparable natural
     *        ordering} of the keys will be used.
     */
    public ConcurrentSkipListMap(Comparator<? super K> comparator) {
        this.comparator = comparator;
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
    private static final VarHandle HEAD;
    private static final VarHandle NEXT;
    private static final VarHandle VAL;
    private static final VarHandle RIGHT;
    static {
        try {
            MethodHandles.Lookup l = MethodHandles.lookup();
            HEAD = l.findVarHandle(ConcurrentSkipListMap.class, "head",
                    Index.class);
            NEXT = l.findVarHandle(Node.class, "next", Node.class);
            VAL = l.findVarHandle(Node.class, "val", Object.class);
            RIGHT = l.findVarHandle(Index.class, "right", Index.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    // For debug
    public long getSumOfKeys() {
        Node<K,V> b, n;
        long keysSum = 0;
        if ((b = baseHead()) == null) {
            return 0;
        }
        while ((n = b.next) != null) {
            K k = n.key;
            if (n.val != null && k != null)
                keysSum += (Integer) k;
            b = n;
        }
        return keysSum;
    }
}
