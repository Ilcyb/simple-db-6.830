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
    private final Map<TransactionId, Set<PageId>> tranPageMap;

    private final LockManager lockManager;

    public BufferPool(int numPages) {
        // some code goes here
        this.numPage = numPages;
        this.pageStore = new LRUMap<>(new ConcurrentHashMap<>());
        this.lockManager = new LockManager();
        this.tranPageMap = new ConcurrentHashMap<>();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
        Page requestedPage = pageStore.get(pid);
        if (requestedPage==null) {
            if (pageStore.size()==numPage)
                evictPage();
            requestedPage = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
            pageStore.put(pid, requestedPage);
        }

        // save relationship between transaction and page
        tranPageMap.computeIfAbsent(tid, k->new HashSet<>()).add(pid);

        if (perm==Permissions.READ_ONLY)
            lockManager.acquireSharedLock(tid, requestedPage);
        else if (perm==Permissions.READ_WRITE)
            lockManager.acquireExclusiveLock(tid, requestedPage);
        else
            throw new IllegalStateException("Not supported type");

        return pageStore.get(pid);
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
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        try {
            lockManager.releaseLock(tid, pid);
        } catch (DbException e) {
            e.printStackTrace();
        }
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
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
        if (!commit) {
            // revert page data
            for (PageId pageId:tranPageMap.get(tid)) {
                Page page = pageStore.get(pageId);
                // clean page of this transaction, have been evict from pageStore
                if (page==null) {
                    continue;
                }
                pageStore.put(pageId, page.getBeforeImage());
            }
        } else {
            // save dirty page data to disk
            try {
                flushPages(tid);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        // release all locks
        try {
            lockManager.releaseAllLock(tid);
        } catch (DbException e) {
            e.printStackTrace();
        }

        tranPageMap.remove(tid);
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
            page.markDirty(true, tid);
            pageStore.put(page.getId(), page);

            if (pageStore.size()>DEFAULT_PAGES)
                evictPage();
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
        for (PageId pageId:tranPageMap.get(tid)) {
            Page page = pageStore.get(pageId);
            // clean page of this transaction, have been evict from pageStore
            if (page==null) {
                continue;
            }
            if (page.isDirty()!=null) {
                flushPage(pageId);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1

        Page cleanPage = null;
        for (LRUMap<PageId, Page>.ValIterator it = pageStore.ValIterator(); it.hasNext(); ) {
            Page page = it.next();
            if (page.isDirty()!=null) {
                continue;
            }
            cleanPage=page;
            break;
        }

        if (cleanPage==null) {
            throw new DbException("There are no clean page to evict");
        }

        PageId removedPid = cleanPage.getId();
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
            entry.item = item;
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

    class ValIterator implements Iterator {
        LRUEntry next = tail.prev;

        @Override
        public boolean hasNext() {
            return next!=head;
        }

        @Override
        public V next() {
            V res = next.item;
            next=next.prev;
            return res;
        }
    }

    public ValIterator ValIterator() {
        return new ValIterator();
    }
}

enum LockType {
    SHARED_LOCK,
    EXCLUSIVE_LOCK,
    NO_LOCK
}

class Lock {
    private final Map<TransactionId, LockType> transactionIdLockTypeMap;
    private final Page page;
    private LockType type;
    private boolean status;

    Lock(Page page) {
        this.page = page;
        this.status = false;
        this.type = LockType.NO_LOCK;
        this.transactionIdLockTypeMap = new ConcurrentHashMap<>();
    }

    public void lock(TransactionId acquireTid, LockType acquireType) throws InterruptedException {

        if (transactionIdLockTypeMap.containsKey(acquireTid) && type==LockType.EXCLUSIVE_LOCK)
            return;

        // lock upgrade from shared lock to exclusive lock
        if (transactionIdLockTypeMap.size()==1
                && transactionIdLockTypeMap.containsKey(acquireTid)
                && type==LockType.SHARED_LOCK
                && acquireType==LockType.EXCLUSIVE_LOCK) {
            type = LockType.EXCLUSIVE_LOCK;
            return;
        }

        if (type == LockType.NO_LOCK) {
            transactionIdLockTypeMap.put(acquireTid, acquireType);
            type = acquireType;
            status = true;
            return;
        }

        if (type == LockType.SHARED_LOCK) {
            if (acquireType == LockType.SHARED_LOCK) {
                transactionIdLockTypeMap.put(acquireTid, acquireType);
                return;
            } else if (acquireType == LockType.EXCLUSIVE_LOCK) {
                synchronized (page) {
                    while (true) {
                        if (type == LockType.NO_LOCK && !status) {
                            transactionIdLockTypeMap.put(acquireTid, acquireType);
                            type = LockType.EXCLUSIVE_LOCK;
                            status = true;
                            return;
                        } else {
                            page.wait();
                        }
                    }
                }
            }
        }

        if (type == LockType.EXCLUSIVE_LOCK) {
            synchronized (page) {
                while (true) {
                    if (type == LockType.NO_LOCK && !status) {
                        transactionIdLockTypeMap.put(acquireTid, acquireType);
                        type = acquireType;
                        status = true;
                        return;
                    } else {
                        page.wait();
                    }
                }
            }
        }

        throw new IllegalStateException("Not supported lock type");
    }

    public synchronized void unlock(TransactionId tid) throws DbException {
        if (!transactionIdLockTypeMap.containsKey(tid)) {
            throw new DbException("Attempt to unlock a lock that does not belong to the transaction");
        }

        if (type==LockType.NO_LOCK) {
            throw new DbException("There are no lock to unlock");
        }

        LockType acquiredLockType = transactionIdLockTypeMap.get(tid);
        if (acquiredLockType!=type) {
            throw new DbException("Lock type not match");
        }

        synchronized (page) {
            transactionIdLockTypeMap.remove(tid);
            if (transactionIdLockTypeMap.isEmpty()) {
                type = LockType.NO_LOCK;
                status = false;
                page.notifyAll();
            } else if (type==LockType.EXCLUSIVE_LOCK) {
                throw new DbException("Shouldn't be here");
            }
        }
    }

    public boolean isFreeLock() {
        return transactionIdLockTypeMap.isEmpty() && type==LockType.NO_LOCK && !status;
    }
}

class LockManager {

    private Map<PageId, Lock> pageRWLockMap;
    private Map<TransactionId, Set<PageId>> transactionMap;

    LockManager() {
        // lockMap 有多个线程同时访问同一个 Page 对应的锁的风险，因此要使用 ConcurrentHashMap
        pageRWLockMap = new ConcurrentHashMap<>();
        transactionMap = new HashMap<>();
    }

    /**
     * acquire shared lock for a page
     * @param tid TransactionId
     * @param page Page
     */
    void acquireSharedLock(TransactionId tid, Page page) {
        transactionAcquirePageLock(tid, page, LockType.SHARED_LOCK);
    }

    /**
     * acquire exclusive lock for a page
     * @param tid TransactionId
     * @param page Page
     */
    void acquireExclusiveLock(TransactionId tid, Page page) {
        transactionAcquirePageLock(tid, page, LockType.EXCLUSIVE_LOCK);
    }

    /**
     * Release page lock for transaction
     * @param tid TransactionId
     * @param pageId PageId
     * @throws DbException
     */
    void releaseLock(TransactionId tid, PageId pageId) throws DbException {
        Lock lock = pageRWLockMap.get(pageId);

        if (lock==null) {
            System.err.printf("(Transaction:%d) does not have lock for (Table:%d,Page:%d)%n",
                    tid.getId(), pageId.getTableId(), pageId.getPageNumber());
            return;
        }

        lock.unlock(tid);
        transactionMap.get(tid).remove(pageId);
    }

    /**
     * release all lock for a transaction
     * @param tid
     * @throws DbException
     */
    void releaseAllLock(TransactionId tid) throws DbException {

        for (PageId pageId:transactionMap.get(tid)) {
            Lock lock = pageRWLockMap.get(pageId);
            lock.unlock(tid);
            if (lock.isFreeLock())
                pageRWLockMap.remove(pageId);
        }

        transactionMap.remove(tid);
    }

    boolean holdsLock(TransactionId tid, PageId pid) {
        return pageRWLockMap.containsKey(pid) && transactionMap.get(tid).contains(pid);
    }

    /**
     *
     * @param tid
     * @param page
     * @param type
     */
    private void transactionAcquirePageLock(TransactionId tid, Page page, LockType type) {
        PageId pid = page.getId();
        Lock lock = pageRWLockMap.computeIfAbsent(pid, k -> new Lock(page));
        Set<PageId> transactionPidSet = transactionMap.computeIfAbsent(tid, k -> new HashSet<PageId>());
        transactionPidSet.add(pid);

        try {
            lock.lock(tid, type);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
