package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    private final int numPage;
    private final LRUMap<PageId, Page> pageStore;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPage = numPages;
        this.pageStore = new LRUMap<>(new ConcurrentHashMap<>());
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page requestedPage = pageStore.get(pid);
        if (requestedPage==null) {
            if (pageStore.size()==numPage)
                evictPage();
            requestedPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageStore.put(pid, requestedPage);
        }
        return requestedPage;
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.insertTuple(tid, t);
        updatePages(tid, pages);
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = t.getRecordId().getPageId().getTableId();
        DbFile file = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = file.deleteTuple(tid, t);
        updatePages(tid, pages);
    }

    private void updatePages(TransactionId tid, List<Page> pages) throws DbException {
        for (Page page: pages) {
            synchronized (page) {
                page.markDirty(true, tid);
                pageStore.put(page.getId(), page);
                if (pageStore.size()>DEFAULT_PAGES)
                    evictPage();
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry:pageStore.entrySet()) {
            Page page = entry.getValue();
            TransactionId tid = page.isDirty();
            if (tid!=null)
                flushPage(entry.getKey());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        pageStore.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
        dbFile.writePage(pageStore.get(pid));
        pageStore.get(pid).markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        for (Map.Entry<PageId, Page> entry:pageStore.entrySet()) {
            Page page = entry.getValue();
            TransactionId transactionId = page.isDirty();
            if (tid==transactionId)
                flushPage(entry.getKey());
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        PageId removedPid = pageStore.getLast();
        try {
            flushPage(removedPid);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        discardPage(removedPid);
    }

}

class LRUMap<K,V> {

    class LRUEntry {
        K key;
        V item;
        LRUEntry prev;
        LRUEntry next;

        LRUEntry() {
            prev=null;
            next=null;
        }

        LRUEntry(K key, V item) {
            this.key = key;
            this.item = item;
            prev=null;
            next=null;
        }
    }

    private final Map<K,LRUEntry> itemMap;
    private final LRUEntry head;
    private final LRUEntry tail;
    private int size;

    LRUMap(Map<K, LRUEntry> itemMap) {
        this.itemMap = itemMap;
        head = new LRUEntry();
        tail = new LRUEntry();
        head.next = tail;
        tail.prev = head;
        size=0;
    }

    V get(K key) {
        LRUEntry entry = itemMap.get(key);
        if (entry!=null) {
            moveInnerEntryToHead(entry);
            return entry.item;
        }
        return null;
    }

    void put(K key, V item) {
        LRUEntry entry = itemMap.get(key);
        if (entry!=null) {
            moveInnerEntryToHead(entry);
            return;
        }
        entry = new LRUEntry(key, item);
        itemMap.put(key, entry);
        moveEntryToHead(entry);
        size++;
    }

    K getLast() {
        return tail.prev.key;
    }

    V remove(K key) {
        LRUEntry entry = itemMap.get(key);
        if (entry==null)
            return null;
        breakFromList(entry);
        itemMap.remove(entry.key);
        size--;
        return entry.item;
    }

    public int size() {
        return size;
    }

    Set<Map.Entry<K, V>> entrySet() {
        Set<Map.Entry<K, LRUEntry>> lruEntrySet = itemMap.entrySet();
        Set<Map.Entry<K, V>> entrySet = new HashSet<>();
        for (Map.Entry<K, LRUEntry> entry:lruEntrySet) {
            entrySet.add(Map.entry(entry.getKey(), entry.getValue().item));
        }
        return entrySet;
    }

    /**
     * move specify entry to head
     * @param entry LRUEntry
     */
    private synchronized void moveEntryToHead(LRUEntry entry) {
        entry.next = head.next;
        head.next.prev = entry;
        head.next = entry;
        entry.prev = head;
    }

    /**
     * separate entry from list
     * @param entry LRUEntry
     */
    private synchronized void breakFromList(LRUEntry entry) {
        entry.prev.next = entry.next;
        entry.next.prev = entry.prev;
        entry.prev = null;
        entry.next = null;
    }

    /**
     * move an entry in list to head of list
     * @param entry LRUEntry
     */
    private synchronized void moveInnerEntryToHead(LRUEntry entry) {
        breakFromList(entry);
        moveEntryToHead(entry);
    }

    /**
     * remove last entry from list
     * @return LRUEntry
     */
    private synchronized LRUEntry removeLastEntryFromList() {
        LRUEntry last = tail.prev;
        breakFromList(last);
        return last;
    }
}
