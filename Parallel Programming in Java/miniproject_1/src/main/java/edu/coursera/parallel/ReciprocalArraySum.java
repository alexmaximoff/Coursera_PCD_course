package edu.coursera.parallel;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value = 0;

        static int SEQ_THRESHOLD = 200000001;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            // TODO
            this.value = 0;
            if(endIndexExclusive - startIndexInclusive < SEQ_THRESHOLD){
                final long t1 = System.currentTimeMillis();
                for (int i = startIndexInclusive; i < endIndexExclusive; i++) {
                    this.value += 1 / input[i];
                }
                final long t2 = System.currentTimeMillis();
                System.out.println(String.format("Thread %s, low=%d, high=%d, time=%d", Thread.currentThread().getName(), startIndexInclusive, endIndexExclusive, t2-t1));
        } else{
                ReciprocalArraySumTask s1 = new ReciprocalArraySumTask(startIndexInclusive, (startIndexInclusive + endIndexExclusive) / 2, input);
                ReciprocalArraySumTask s2 = new ReciprocalArraySumTask((startIndexInclusive + endIndexExclusive) / 2, endIndexExclusive, input);

                //s1.fork();
                //s2.compute();
                //s1.join();

                invokeAll(s1, s2);

                this.value = s1.value + s2.value;
            }
            }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        ForkJoinPool pool = new ForkJoinPool(2);
        assert input.length % 2 == 0;

        double sum = 0;

        ReciprocalArraySumTask s1 = new ReciprocalArraySumTask(0, input.length / 2, input);
        ReciprocalArraySumTask s2 = new ReciprocalArraySumTask(input.length / 2, input.length, input);

        s1.fork();
        s2.compute();
        s1.join();

        sum = s1.getValue() + s2.getValue();

        return sum;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {
        double sum = 0;

        ForkJoinPool pool = new ForkJoinPool();

        int concurentTask = 12;

        ReciprocalArraySumTask[] task = new ReciprocalArraySumTask[concurentTask];
        // Compute sum of reciprocals of array elements
        for (int i = 0; i < concurentTask; i++) {
            final long start = System.currentTimeMillis();
            int low = getChunkStartInclusive(i, concurentTask, input.length);
            int high = getChunkEndExclusive(i, concurentTask, input.length);

            task[i] = new ReciprocalArraySumTask(low, high, input);
            pool.execute(task[i]);
            final long end = System.currentTimeMillis();
            System.out.println(String.format("Thread=%s, Task %d, low=%d, high=%d, time created=%d, %s", Thread.currentThread().getName(), i, low, high, end - start, pool.toString()));
        }

        for (int i = 0; i < concurentTask; i++) {
            final long start = System.currentTimeMillis();
            task[i].join();
            final long end = System.currentTimeMillis();
            System.out.println(String.format("Thread=%s, Task %d time awaited=%d", Thread.currentThread().getName(), i, end - start));
            sum += task[i].getValue();
        }

        System.out.println(pool.toString());

        double sum1 = 0;
        /*
        for (int c = 12; c>0; c--) {
            sum1 = 0;
            final long t1 = System.currentTimeMillis();
            // Compute sum of reciprocals of array elements
            int low = 0;
            int high = (input.length + c - 1) / c;

            ReciprocalArraySumTask task1 = new ReciprocalArraySumTask(low, high, input);
            pool.invoke(task1);

            final long t2 = System.currentTimeMillis();

            System.out.println(String.format("Thread=%s, Chunk=1/%d, time=%d", Thread.currentThread().getName(), c, t2 - t1));
        }
        */

        /*
        for (int i = 0; i < concurentTask; i++) {
            final long start = System.currentTimeMillis();
            int low = getChunkStartInclusive(i, concurentTask, input.length);
            int high = getChunkEndExclusive(i, concurentTask, input.length);

            ReciprocalArraySumTask task1 = new ReciprocalArraySumTask(low, high, input);
            pool.invoke(task1);
            final long end = System.currentTimeMillis();
            System.out.println(String.format("Thread=%s, Task %d, low=%d, high=%d, time created=%d, %s", Thread.currentThread().getName(), i, low, high, end - start, pool.toString()));
        }
        */

        for (int i = 1; i < 13; i++) {
            final long start = System.currentTimeMillis();
            int low = 0;
            int high = (input.length + i - 1) / i;

            ReciprocalArraySumTask task1 = new ReciprocalArraySumTask(low, high, input);
            pool.invoke(task1);
            final long end = System.currentTimeMillis();
            System.out.println(String.format("Thread=%s, Task %d, low=%d, high=%d, time created=%d, %s", Thread.currentThread().getName(), i, low, high, end - start, pool.toString()));
        }


        return sum;
    }
}
