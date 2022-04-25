package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Context groupContext;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.groupContext = new Context();
        switch (what) {
            case MIN:
                this.groupContext.setStrategy(new MIN());
                break;
            case MAX:
                this.groupContext.setStrategy(new MAX());
                break;
            case AVG:
                this.groupContext.setStrategy(new AVG());
                break;
            case SUM:
                this.groupContext.setStrategy(new SUM());
                break;
            case COUNT:
                this.groupContext.setStrategy(new COUNT());
                break;
            default:
                throw new UnsupportedOperationException("Not implement");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gfield;
        if (gbfield==NO_GROUPING)
            gfield = null;
        else
            gfield = tup.getField(gbfield);
        groupContext.strategy.strategyMethod(groupContext.groupResult, gfield, tup.getField(afield));
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        TupleDesc aggTd;
        if (gbfield==NO_GROUPING) {
            aggTd = new TupleDesc(new Type[]{Type.INT_TYPE},
                    new String[]{"aggregateVal"});
        } else {
            aggTd = new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE},
                    new String[]{"groupVal", "aggregateVal"});
        }
        List<Tuple> tuples = new ArrayList<>();
        for (Map.Entry<Field, Integer> entry:groupContext.groupResult.entrySet()) {
            Tuple newTuple = new Tuple(aggTd);
            if (gbfield==NO_GROUPING)
                newTuple.setField(0, new IntField(entry.getValue()));
            else {
                newTuple.setField(0, entry.getKey());
                newTuple.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(newTuple);
        }
        return new TupleIterator(aggTd, tuples);
    }

    private class Context {
        Strategy strategy;

        HashMap<Field, Integer> groupResult;

        Context() {
            groupResult = new HashMap<>();
        }

        void setStrategy(Strategy strategy) {
            this.strategy = strategy;
        }
    }

    private interface Strategy {
        void strategyMethod(HashMap<Field, Integer> groupResult, Field gfield, Field afield);
    }

    private class COUNT implements Strategy {

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field field, Field afield) {
            int count = groupResult.getOrDefault(field, 0)+1;
            groupResult.put(field, count);
        }
    }

    private class SUM implements Strategy {

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field field, Field afield) {
            int sum = groupResult.getOrDefault(field, 0)+((IntField)afield).getValue();
            groupResult.put(field, sum);
        }
    }

    private class AVG implements Strategy {

        private HashMap<Field, Integer> sum = new HashMap<>();
        private HashMap<Field, Integer> count = new HashMap<>();

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field field, Field afield) {
            int groupSum = sum.getOrDefault(field, 0)+((IntField)afield).getValue();
            sum.put(field, groupSum);
            int groupCount = count.getOrDefault(field, 0)+1;
            count.put(field, groupCount);
            groupResult.put(field, groupSum/groupCount);
        }
    }

    private class MIN implements Strategy {

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field field, Field afield) {
            int minVal = Math.min(((IntField)afield).getValue(), groupResult.getOrDefault(field, Integer.MAX_VALUE));
            groupResult.put(field, minVal);
        }
    }

    private class MAX implements Strategy {

        @Override
        public void strategyMethod(HashMap<Field, Integer> groupResult, Field field, Field afield) {
            int maxVal = Math.max(((IntField)afield).getValue(), groupResult.getOrDefault(field, Integer.MIN_VALUE));
            groupResult.put(field, maxVal);
        }
    }
}
