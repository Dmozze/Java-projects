package info.kgeorgiy.ja.mozzhevilov.concurrent;

import info.kgeorgiy.java.advanced.concurrent.AdvancedIP;
import info.kgeorgiy.java.advanced.mapper.ParallelMapper;

import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;


public class IterativeParallelism implements AdvancedIP {
  final private ParallelMapper mapper;

  public IterativeParallelism() {
    mapper = null;
  }

  public IterativeParallelism(ParallelMapper mapper) {
    this.mapper = mapper;
  }

  private <T, R> R parallelIt(int threads,
                         List<T> values,
                         Function<? super Stream<T>, R> task,
                         Function<? super Stream<R>, R> collector) throws InterruptedException {
    if (threads <= 0) {
      throw new IllegalArgumentException("thread number should be >= 0");
    }
    final List<Stream<T>> subTasks = new ArrayList<>();
    List<R> res;
    threads = Math.min(values.size(), threads);
    int blockSize = values.size() / threads + 1;
    final int rest = values.size() % threads;
    for (int i = 0, pos = 0; i < threads; ++i, pos += blockSize) {
      if (rest == i) {
        blockSize--;
      }
      subTasks.add(values.subList(pos, pos + blockSize).stream());
    }
    if (mapper != null) {
      res = mapper.map(task, subTasks);
    } else {
      final List<Thread> workers = new ArrayList<>();
      res = new ArrayList<>(Collections.nCopies(threads, null));
      for (int i = 0; i < threads; i++) {
        int finalI = i;
        Thread tempThread = new Thread(() -> res.set(finalI, task.apply(subTasks.get(finalI))));
        workers.add(tempThread);
        tempThread.start();
      }
      InterruptedException exception = null;
      for (Thread thread : workers) {
        try {
          thread.join();
        } catch (InterruptedException e) {
          if (exception == null) {
            exception = new InterruptedException("some threads didn't join");
          }
          exception.addSuppressed(e);
          for (Thread toStop : workers) {
            // :NOTE: your current thread is still alive
            if (toStop.isAlive()) {
              toStop.interrupt();
            }
          }
        }
      }
      if (exception != null) {
        throw exception;
      }
    }
    return collector.apply(res.stream());
  }

  @Override
  public <T> T reduce(int threads, List<T> values, Monoid<T> monoid) throws InterruptedException {
    return parallelIt(threads, values, stream -> stream.reduce(monoid.getIdentity(), monoid.getOperator()), stream -> stream.reduce(monoid.getIdentity(), monoid.getOperator()));
  }

  @Override
  public <T, R> R mapReduce(int threads, List<T> values, Function<T, R> lift, Monoid<R> monoid) throws InterruptedException {
    return parallelIt(threads, values,
            stream -> stream.map(lift).reduce(monoid.getIdentity(), monoid.getOperator()),
            stream -> stream.reduce(monoid.getIdentity(), monoid.getOperator()));
  }

  @Override
  public String join(int threads, List<?> values) throws InterruptedException {
    return parallelIt(threads, values,
            stream -> stream.map(Object::toString).collect(Collectors.joining()),
            stream -> stream.collect(Collectors.joining()));
  }

  @Override
  public <T> List<T> filter(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
    return parallelIt(threads, values,
            stream -> stream.filter(predicate).collect(Collectors.toList()),
            stream -> stream.flatMap(Collection::stream).collect(Collectors.toList()));
  }

  @Override
  public <T, U> List<U> map(int threads, List<? extends T> values, Function<? super T, ? extends U> f) throws InterruptedException {
    // :NOTE: intermediate structures
    return parallelIt(threads, values,
            stream -> stream.map(f).collect(Collectors.toList()),
            stream -> stream.flatMap(Collection::stream).collect(Collectors.toList()));
  }

  @Override
  public <T> T maximum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
    // :NOTE: you can do without it
    if (values == null || values.isEmpty()) {
      throw new IllegalArgumentException("Values are null or empty");
    }
    Function<Stream<? extends T>, T> streamMax = stream -> stream.max(comparator).get();
    return parallelIt(threads, values, streamMax, streamMax);
  }

  @Override
  public <T> T minimum(int threads, List<? extends T> values, Comparator<? super T> comparator) throws InterruptedException {
    return maximum(threads, values, Collections.reverseOrder(comparator));
  }

  @Override
  public <T> boolean all(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
    return parallelIt(threads, values,
            stream -> stream.allMatch(predicate),
            stream -> stream.allMatch(Boolean::booleanValue));
  }

  @Override
  public <T> boolean any(int threads, List<? extends T> values, Predicate<? super T> predicate) throws InterruptedException {
    return !all(threads, values, predicate.negate());
  }
}