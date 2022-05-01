package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File backedFile;

    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.backedFile = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return backedFile;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return backedFile.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            byte[] pageBytes = randomAccessHeapFile(
                    pid.getPageNumber()*BufferPool.getPageSize(), BufferPool.getPageSize());
            return new HeapPage((HeapPageId) pid, pageBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int pageNo = page.getId().getPageNumber();
        if (pageNo>numPages())
            throw new IllegalStateException("over file");
        RandomAccessFile raf = new RandomAccessFile(backedFile, "rw");
        raf.seek((long) pageNo *BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (backedFile.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> modifiedPages = new ArrayList<>();
        HeapPage modifiedPage = null;
        modifiedPage = getFreePage(tid);

        if (modifiedPage==null) { // all pages are full
            synchronized (this) {
                // double check
                modifiedPage = getFreePage(tid);
                if (modifiedPage==null) {
                    BufferedOutputStream bufferedOutputStream =
                            new BufferedOutputStream(new FileOutputStream(backedFile, true));
                    byte[] emptyPageData = HeapPage.createEmptyPageData();
                    bufferedOutputStream.write(emptyPageData);
                    bufferedOutputStream.close();

                    HeapPageId newPageId = new HeapPageId(getId(), numPages()-1);
                    modifiedPage = (HeapPage) Database.getBufferPool().getPage(tid, newPageId, Permissions.READ_WRITE);
                }
            }
        }

        modifiedPage.insertTuple(t);
        modifiedPages.add(modifiedPage);
        return modifiedPages;
    }

    /**
     * get the page with extra space
     * @param tid
     * @return HeapPage
     * @throws TransactionAbortedException
     * @throws DbException
     */
    private HeapPage getFreePage(TransactionId tid) throws TransactionAbortedException, DbException {
        HeapPage modifiedPage = null;
        BufferPool bufferPool = Database.getBufferPool();
        for (int i = 0; i < numPages(); i++) {
            HeapPageId pageId= new HeapPageId(getId(), i);
            HeapPage page = (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_ONLY);
            if (page.getNumEmptySlots()!=0) {
                modifiedPage= (HeapPage) bufferPool.getPage(tid, pageId, Permissions.READ_WRITE);
                break;
            }
            // unlock page lock after scan finished
            bufferPool.unsafeReleasePage(tid, pageId);
        }
        return modifiedPage;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        HeapPage page = (HeapPage) Database.getBufferPool().
                getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        return new ArrayList<>(List.of(page));
    }

    class HeapFileIterator implements DbFileIterator{

        int totalPage = numPages();
        int pageCur = 0;
        Iterator<Tuple> iterator;
        TransactionId tid;
        Tuple next;
        boolean open;

        HeapFileIterator(TransactionId tid) {
            this.tid = tid;
            this.next = null;
            this.open = false;
        }

        Iterator<Tuple> getIterator(int pageNo) throws TransactionAbortedException, DbException {
            if (pageNo>totalPage||pageNo<0)
                throw new NoSuchElementException();
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,
                    new HeapPageId(getId(), pageNo), Permissions.READ_ONLY);
            return page.iterator();
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            if (totalPage<=0)
                throw new NoSuchElementException();
            open = true;
            pageCur = 0;
            iterator = getIterator(pageCur++);
            next = null;
            while (next==null&&iterator!=null) {
                if (iterator.hasNext()) {
                    next=iterator.next();
                } else {
                    if (pageCur<totalPage) {
                        iterator=getIterator(pageCur++);
                    }else {
                        return;
                    }
                }
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (!open)
                return false;
            return next!=null;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if (!open)
                throw new NoSuchElementException("Not open yet.");
            if (next==null)
                throw new NoSuchElementException();
            Tuple ans = next;
            next=null;
            while (next==null&&iterator!=null) {
                if (iterator.hasNext()) {
                    next=iterator.next();
                } else {
                    if (pageCur<totalPage) {
                        iterator=getIterator(pageCur++);
                    }else {
                        break;
                    }
                }
            }

            return ans;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            if (!open)
                throw new IllegalStateException("Not open yet.");
            open();
        }

        @Override
        public void close() {
            open=false;
            iterator = null;
            next=null;
            pageCur=0;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

    private byte[] randomAccessHeapFile(int offset, int len) throws IOException {
        byte[] dst = new byte[len];
        RandomAccessFile raf = new RandomAccessFile(backedFile, "r");
        raf.seek(offset);
        int byteLength = raf.read(dst, 0, len);
        if (byteLength != len)
            throw new NoSuchElementException();
        raf.close();
        return dst;
    }

}

