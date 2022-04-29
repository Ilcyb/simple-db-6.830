package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int[] buckets;
    private int minVal;
    private int maxVal;
    private double bucketWidth;
    private int ntups;
    private Context context;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        this.buckets = new int[buckets];
        minVal = min;
        maxVal = max;
        bucketWidth = ((maxVal-minVal+1)*1.0D) / buckets;
        ntups = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        int bucketIdx = computeBucketIdx(v);
        buckets[bucketIdx]++;
        ntups++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        context = new Context(v);
        switch (op) {
            case EQUALS:
                context.setStrategy(new Equals());
                break;
            case GREATER_THAN:
                context.setStrategy(new GreaterThan());
                break;
            case LESS_THAN:
                context.setStrategy(new LessThan());
                break;
            case GREATER_THAN_OR_EQ:
                context.setStrategy(new GreateThanOrEquals());
                break;
            case LESS_THAN_OR_EQ:
                context.setStrategy(new LessThanOrEquals());
                break;
            case NOT_EQUALS:
                context.setStrategy(new NotEquals());
                break;
            default:
                throw new IllegalStateException("Not supported op");
        }

        return context.strategyMethod();
    }

    private class Context {

        private Strategy strategy;
        private int v;

        public Context(int v) {
            this.v = v;
        }

        void setStrategy(Strategy strategy) {
            this.strategy = strategy;
        }

        double strategyMethod() {
            return strategy.strategyMethod(v);
        }
    }

    private interface Strategy {
        double strategyMethod(int v);
    }

    private class Equals implements Strategy {

        @Override
        public double strategyMethod(int v) {
            int bucketIdx = computeBucketIdx(v);
            return (buckets[bucketIdx]/bucketWidth)/ntups;
        }
    }

    private class GreaterThan implements Strategy {

        @Override
        public double strategyMethod(int v) {
            if (v<minVal)
                return 1.0;
            if (v>maxVal)
                return 0;
            int bucketIdx = computeBucketIdx(v);
            double bRight = (bucketIdx+1)*bucketWidth;
            double bFrac = buckets[bucketIdx]/(ntups*1D);
            double bPart = (bRight-v)/bucketWidth;
            double selectivity = bFrac * bPart;
            for (int i = bucketIdx+1; i < buckets.length; i++) {
                selectivity += buckets[i]/(ntups*1D);
            }
            return selectivity;
        }
    }

    private class LessThan implements Strategy {

        @Override
        public double strategyMethod(int v) {
            if (v<minVal)
                return 0;
            if (v>maxVal)
                return 1.0;
            int bucketIdx = computeBucketIdx(v);
            double bLeft = bucketIdx*bucketWidth;
            double bFrac = buckets[bucketIdx]/(ntups*1D);
            double bPart = (v-bLeft)/bucketWidth;
            double selectivity = bFrac * bPart;
            for (int i = 0; i < bucketIdx; i++) {
                selectivity += buckets[i]/(ntups*1D);
            }
            return selectivity;
        }
    }

    private class LessThanOrEquals implements Strategy {

        @Override
        public double strategyMethod(int v) {
            if (v<minVal)
                return 0;
            if (v>maxVal)
                return 1.0;
            int bucketIdx = computeBucketIdx(v);
            double selectivity = (buckets[bucketIdx]/bucketWidth)/ntups;
            double bLeft = bucketIdx*bucketWidth;
            double bFrac = buckets[bucketIdx]/(ntups*1D);
            double bPart = (v-bLeft)/bucketWidth;
            selectivity += bFrac * bPart;
            for (int i = 0; i < bucketIdx; i++) {
                selectivity += buckets[i]/(ntups*1D);
            }
            return selectivity;
        }
    }

    private class GreateThanOrEquals implements Strategy {

        @Override
        public double strategyMethod(int v) {
            if (v<minVal)
                return 1.0;
            if (v>maxVal)
                return 0;
            int bucketIdx = computeBucketIdx(v);
            double selectivity = (buckets[bucketIdx]/bucketWidth)/ntups;
            double bRight = (bucketIdx+1)*bucketWidth;
            double bFrac = buckets[bucketIdx]/(ntups*1D);
            double bPart = (bRight-v)/bucketWidth;
            selectivity += bFrac * bPart;
            for (int i = bucketIdx+1; i < buckets.length; i++) {
                selectivity += buckets[i]/(ntups*1D);
            }
            return selectivity;
        }
    }

    private class NotEquals implements Strategy {

        @Override
        public double strategyMethod(int v) {
            int bucketIdx = computeBucketIdx(v);
            return 1-(buckets[bucketIdx]/bucketWidth)/ntups;
        }
    }

    private int computeBucketIdx(int v) {
        return (int) Math.floor((v-minVal)/bucketWidth);
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" +
                "buckets=" + Arrays.toString(buckets) +
                ", minVal=" + minVal +
                ", maxVal=" + maxVal +
                ", bucketWidth=" + bucketWidth +
                ", ntups=" + ntups +
                '}';
    }
}
