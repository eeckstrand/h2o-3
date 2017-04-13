package hex.sensitivityanalysis;

import jsr166y.CountedCompleter;
import water.H2O;
import java.util.concurrent.atomic.AtomicInteger;

public class ParallelTasks<T extends H2O.H2OCountedCompleter<T>> extends H2O.H2OCountedCompleter {
    private final AtomicInteger _ctr; // Concurrency control
    private static int DEFAULT_MAX_PARALLEL_TASKS = -1;    // Max number of concurrent columns
    private final T[] _tasks;         // task holder (will be 1 per column)
    transient private int _maxParallelTasks;

    public ParallelTasks(T[] tasks) {
        this(tasks, DEFAULT_MAX_PARALLEL_TASKS);
    }

    public ParallelTasks(T[] tasks, int maxParallelTasks) {
        _maxParallelTasks = maxParallelTasks > 0 ? maxParallelTasks : H2O.SELF._heartbeat._num_cpus;
        _ctr = new AtomicInteger(_maxParallelTasks - 1);
        _tasks = tasks;
    }

    @Override public void compute2() {
        final int nTasks = _tasks.length;
        addToPendingCount(nTasks-1);
        for (int i=0; i < Math.min(_maxParallelTasks, nTasks); ++i) asyncVecTask(i);
    }

    private void asyncVecTask(final int task) {
        _tasks[task].setCompleter(new Callback());
        _tasks[task].fork();
    }

    private class Callback extends H2O.H2OCallback{
        public Callback(){super(ParallelTasks.this);}
        @Override public void callback(H2O.H2OCountedCompleter cc) {
            int i = _ctr.incrementAndGet();
            if (i < _tasks.length)
                asyncVecTask(i);
        }

        @Override
        public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
            ex.printStackTrace();
            return super.onExceptionalCompletion(ex, caller);
        }
    }
}
