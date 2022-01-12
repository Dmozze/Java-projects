package info.kgeorgiy.ja.mozzhevilov.concurrent;

import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;

public class ParallelMapperImpl implements ParallelMapper {

  private final Queue<Runnable> queueOfTasks;
  private final List<Thread> listOfThreads;
  private final Set<ResultCollector<?>> setOfCollectors;
  private volatile boolean volatileBooleanClose;

  public ParallelMapperImpl(final int listOfThreadsCount) {
    if (listOfThreadsCount <= 0) {
      throw new IllegalArgumentException("Count of thread must be > 0");
    }
    setOfCollectors = new HashSet<>();
    queueOfTasks = new ArrayDeque<>();
    listOfThreads = new ArrayList<>();
    volatileBooleanClose = false;
    for (int i = 0; i < listOfThreadsCount; i++) {
      final Thread thread = new Thread(() -> {
        try {
          while (!Thread.interrupted()) {
            pollTask().run();
          }
        } catch (final InterruptedException ignored) {
        } finally {
          Thread.currentThread().interrupt();
        }
      });
      listOfThreads.add(thread);
      thread.start();
    }
  }

  private Runnable pollTask() throws InterruptedException {
    synchronized (queueOfTasks) {
      while (queueOfTasks.size() == 0) {
        queueOfTasks.wait();
      }
      return queueOfTasks.poll();
    }
  }

  private void addTask(final Runnable task) {
    synchronized (queueOfTasks) {
      queueOfTasks.add(task);
      queueOfTasks.notify();
    }
  }

  private class ResultCollector<R> {
    private final List<R> res;
    private int done;
    private RuntimeException exception;
    private boolean needFinish;

    ResultCollector(final int size) {
      res = new ArrayList<>(Collections.nCopies(size, null));
      synchronized (ParallelMapperImpl.this) {
        if (!volatileBooleanClose)
          setOfCollectors.add(ResultCollector.this);
      }
    }

    synchronized void addException(final RuntimeException e) {
      if (needFinish) {
        return;
      }

      if (exception == null) {
        exception = e;
      } else {
        exception.addSuppressed(e);
      }
    }

    synchronized void shutdown() {
      needFinish = true;
      notify();
    }

    synchronized void set(final int pos, final R data) {
      if (needFinish) {
        return;
      }

      res.set(pos, data);
      if (++done == res.size()) {
        notify();
      }
    }

    synchronized List<R> get() throws InterruptedException {
      while (done < res.size() && !needFinish) {
        wait();
      }
      synchronized (ParallelMapperImpl.this) {
        setOfCollectors.remove(ResultCollector.this);
      }
      if (exception != null) {
        throw exception;
      }
      return res;
    }
  }

  @Override
  public <T, R> List<R> map(final Function<? super T, ? extends R> f, final List<? extends T> args) throws InterruptedException {
    if (volatileBooleanClose) {
      throw new RuntimeException("Mapper close");
    }
    final ResultCollector<R> collector = new ResultCollector<>(args.size());
    for (int i = 0; i < args.size(); i++) {
      final int pos = i;
      addTask(() -> {
        try {
          collector.set(pos, f.apply(args.get(pos)));
        } catch (final RuntimeException e) {
          collector.addException(e);
        }
      });
    }
    return collector.get();
  }

  @Override
  synchronized public void close() {
    volatileBooleanClose = true;
    listOfThreads.forEach(Thread::interrupt);
    for (int i = 0; i < listOfThreads.size(); i++) {
      try {
        listOfThreads.get(i).join();
      } catch (final InterruptedException ignored) {
        i--;
      }
    }
    synchronized (this) {
      setOfCollectors.forEach(ResultCollector::shutdown);
    }
  }
}