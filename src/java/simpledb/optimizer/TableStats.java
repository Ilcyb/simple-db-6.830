package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public int tupleNum;
    public int pageNum;
    public Map<Integer, Integer> maxValMap;
    public Map<Integer, Integer> minValMap;
    public int tableId;
    public int scanCostTime;
    private Map<Integer, IntHistogram> intHistogramMap;
    private Map<Integer, StringHistogram> stringHistogramMap;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        HeapFile dbFile = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc td = Database.getCatalog().getTupleDesc(tableid);
        int fieldNums = td.numFields();
        this.tableId = tableid;
        this.pageNum = dbFile.numPages();
        this.scanCostTime = pageNum * ioCostPerPage;
        this.intHistogramMap = new HashMap<>();
        this.stringHistogramMap = new HashMap<>();
        this.maxValMap = new HashMap<>();
        this.minValMap = new HashMap<>();
        DbFileIterator iterator = dbFile.iterator(null);
        try {
            iterator.open();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < fieldNums; i++) {
                    IntField field = (IntField) tuple.getField(i);
                    maxValMap.put(i,
                            Math.max(maxValMap.getOrDefault(i, Integer.MIN_VALUE), field.getValue()));
                    minValMap.put(i,
                            Math.min(minValMap.getOrDefault(i, Integer.MAX_VALUE), field.getValue()));
                }
                tupleNum++;
            }

            for (int i = 0; i < fieldNums; i++) {
                if (td.getFieldType(i).equals(Type.INT_TYPE)) {
                    int maxVal = maxValMap.get(i);
                    int minVal = minValMap.get(i);
                    int bucketNum = Math.min(maxVal - minVal + 1, NUM_HIST_BINS);
                    intHistogramMap.put(i, new IntHistogram(bucketNum, minVal, maxVal));

                }
                else if (td.getFieldType(i).equals(Type.STRING_TYPE)) {
                    stringHistogramMap.put(i, new StringHistogram(NUM_HIST_BINS));
                }
            }

            iterator.rewind();
            while (iterator.hasNext()) {
                Tuple tuple = iterator.next();
                for (int i = 0; i < fieldNums; i++) {
                    Field field = tuple.getField(i);
                    Type fieldType = field.getType();
                    if (fieldType.equals(Type.INT_TYPE))
                        intHistogramMap.get(i).addValue(((IntField)field).getValue());
                    else if (fieldType.equals(Type.STRING_TYPE))
                        stringHistogramMap.get(i).addValue(((StringField)field).getValue());
                }
            }
        } catch (TransactionAbortedException | DbException e) {
            e.printStackTrace();
        } finally {
            iterator.close();
        }
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return scanCostTime;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (tupleNum*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        Type constantType = constant.getType();
        if (constantType.equals(Type.INT_TYPE)) {
            return intHistogramMap.get(field).estimateSelectivity(op, ((IntField)constant).getValue());
        } else if (constantType.equals(Type.STRING_TYPE)) {
            return stringHistogramMap.get(field).estimateSelectivity(op, ((StringField)constant).getValue());
        }
        return 1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tupleNum;
    }

}
