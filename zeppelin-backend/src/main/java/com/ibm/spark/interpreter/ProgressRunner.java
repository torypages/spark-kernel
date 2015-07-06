package com.ibm.spark.interpreter;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a simple utility to gradually increase a counter representing
 * progress to Zeppelin.
 */
public class ProgressRunner {
    private static final int MIN_PROGRESS = 0;
    private static final int MAX_PROGRESS = 100;
    private static final long PROGRESS_INCREMENT = 1000L;

    /** Represents the progress on the current block of execution. */
    private AtomicInteger progress = new AtomicInteger(0);

    /** Represents the flag that is checked regarding incrementing progress. */
    private AtomicBoolean incrementProgress = new AtomicBoolean(false);

    private final boolean wrapProgress;

    /** When flag is set, increments progress up to 100 once per second. */
    private final Thread progressThread = new Thread(new Runnable() {
        @Override
        public void run() {
        while (!Thread.interrupted()) try {
            if (incrementProgress.get() && getProgress() < MAX_PROGRESS) {
                progress.incrementAndGet();
            } else if (wrapProgress) {
                progress.set(MIN_PROGRESS);
            }

            Thread.sleep(PROGRESS_INCREMENT);
        } catch (InterruptedException ex) {
            return; // Should exit if interrupted
        }
        }
    });

    /** Creates a new progress runner that wraps progress increments. */
    public ProgressRunner() {
        this(true);
    }

    /**
     * Creates a new progress runner.
     *
     * @param wrapProgress If true, wraps progress once it exceeds 100 by
     *                     starting over at 0
     */
    public ProgressRunner(boolean wrapProgress) {
        this.wrapProgress = wrapProgress;
        progressThread.start();
    }

    public void stop() {
        progressThread.interrupt();
    }

    public void setProgress(int newProgress) {
        progress.set(newProgress);
    }

    public int getProgress() {
        return progress.intValue();
    }

    public void turnOnIncrement() {
        incrementProgress.set(true);
    }

    public void turnOffIncrement() {
        incrementProgress.set(false);
    }

    public void clearProgress() {
        setProgress(MIN_PROGRESS);
    }

    public void maximizeProgress() {
        setProgress(MAX_PROGRESS);
    }

    public void clearIfMax() {
        progress.compareAndSet(MAX_PROGRESS, MIN_PROGRESS);
    }
}
