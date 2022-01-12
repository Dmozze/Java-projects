package info.kgeorgiy.ja.mozzhevilov.crawler;

import info.kgeorgiy.java.advanced.crawler.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.*;

import java.util.stream.Collectors;

public class WebCrawler implements AdvancedCrawler {
  private final Downloader downloader;
  private final int perHost;
  private final ExecutorService downloadersPool;
  private final ExecutorService extractorsPool;
  private final Map<String, HostQueue> hostQueueMap;
  private final Map<Document, String> docsUrl;
  private final Set<String> allowedHosts;
  private final static int AWAIT_TERM_SEC = 60;

  public WebCrawler(final Downloader downloader, final int downloaders, final int extractors, final int perHost) {
    this.downloader = downloader;
    downloadersPool = Executors.newFixedThreadPool(downloaders);
    extractorsPool = Executors.newFixedThreadPool(extractors);
    hostQueueMap = new ConcurrentHashMap<>();
    docsUrl = new ConcurrentHashMap<>();
    allowedHosts = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());
    this.perHost = perHost;
  }

  private String getDomain(String url){
    return getURI(url).getHost();
  }

  private URI getURI(String url){
    URI uri = null;
    try {
      uri = new URI(url);
    } catch (URISyntaxException e) {
      e.printStackTrace();
    }
    assert uri != null;
    return uri;
  }

  @Override
  public Result download(String url, int depth, List<String> hosts) {
    if (hosts.size() == 0){
      return new Result(new ArrayList<>(), new ConcurrentHashMap<>());
    }
    for (String host : hosts) {
      allowedHosts.add(getURI(host).toString());
    }
    return new answerCollector().download(url, depth);
  }

  private class answerCollector {
    private final Queue<Future<?>> running;
    private final Set<String> downloaded;
    private final Set<String> visited;
    private final Phaser phaser;
    private final Map<String, IOException> errors;

    answerCollector() {
      downloaded = Collections.newSetFromMap(new ConcurrentHashMap<>());
      errors = new ConcurrentHashMap<>();
      visited = Collections.newSetFromMap(new ConcurrentHashMap<>());
      running = new ConcurrentLinkedQueue<>();
      phaser = new Phaser(0);
    }

    public void downloadPage(final String url, final Collection<Document> downloadedDocs ) {
      try {
        final Document doc = downloader.download(url);
        downloadedDocs.add(doc);
        docsUrl.put(doc, url);
        downloaded.add(url);
      } catch (final IOException e) {
        errors.put(url, e);
      }
    }

    public void Add(final String url, final Queue<Document> downloadedDocs) {
      final String host;
      try {
        host = URLUtils.getHost(url);
      } catch (final MalformedURLException e) {
        errors.put(url, e);
        return;
      }
      hostQueueMap.computeIfAbsent(host, k -> new HostQueue())
              .addAndProcessTask(() -> downloadPage(url, downloadedDocs), this);
    }

    public List<String> extractor(final Document doc) {
      List<String> res = Collections.emptyList();
      try {
        res =  doc.extractLinks();
      } catch (final IOException e) {
        errors.put(docsUrl.get(doc), e);
      }
      return res;
    }


    public Result download(String url, int depth) {
      final List<Document> layer = new ArrayList<>();
      final Queue<Document> nextLayer = new ConcurrentLinkedQueue<>();
      if (allowedHosts.size() == 0 || allowedHosts.contains(getDomain(url))) {
        downloadPage(url, layer);
        visited.add(url);
      }
      for (int curDepth = 1; !layer.isEmpty() && curDepth < depth; curDepth++) {
        layer.stream().map(doc -> extractorsPool.submit(() -> extractor(doc).forEach(u -> {
          if (!visited.contains(u) && (allowedHosts.size() == 0 || allowedHosts.contains(getDomain(u)))) {
            visited.add(u);
            phaser.register();
            Add(u, nextLayer);
          }
        })))
                .collect(Collectors.toList())
                .forEach(WebCrawler::getFromFuture);
        phaser.awaitAdvance(0);
        running.forEach(WebCrawler::getFromFuture);
        layer.clear();
        running.clear();
        layer.addAll(nextLayer);
        nextLayer.clear();
        layer.forEach(docsUrl::remove);
      }
      return new Result(new ArrayList<>(downloaded), errors);
    }
  }

  private class HostQueue {
    final Queue<Runnable> queue;
    final Queue<answerCollector> collectors;
    int free;

    public HostQueue() {
      queue = new ArrayDeque<>();
      collectors = new ArrayDeque<>();
      free = perHost;
    }

    public synchronized void addAndProcessTask(final Runnable task, final answerCollector collector) {
      queue.add(() -> {
        task.run();
        collector.phaser.arriveAndDeregister();
        synchronized (HostQueue.this) {
          free++;
          processTasks();
        }
      });
      collectors.add(collector);
      processTasks();
    }

    public synchronized void processTasks() {
      if (free > 0 && !queue.isEmpty() && !collectors.isEmpty()) {
        free--;
        collectors.poll().running.add(downloadersPool.submit(Objects.requireNonNull(queue.poll())));
      }
    }
  }

  private static <T> void getFromFuture(final Future<T> elem) {
    try {
      elem.get();
    } catch (final InterruptedException | ExecutionException e) {
      // pass
    }
  }

  @Override
  public Result download(final String url, final int depth) {
    return new answerCollector().download(url, depth);
  }

  private void awaitTerm(final ExecutorService executorService) {
    try {
      executorService.awaitTermination(AWAIT_TERM_SEC, TimeUnit.SECONDS);
    } catch (final InterruptedException ignored) {
    }
  }

  @Override
  public void close() {
    downloadersPool.shutdown();
    extractorsPool.shutdown();

    awaitTerm(downloadersPool);
    awaitTerm(extractorsPool);
  }

  public static void main(final String[] args) {
    if (args == null) {
      return;
    }
    if (args.length < 1 || args.length > 5) {
      return;
    }
    for (String arg : args) {
      if (arg == null) {
        return;
      }
    }

    final int[] params = new int[4];
    params[0] = 1;
    params[1] = 10;
    params[2] = 10;
    params[3] = 10;

    try {
      for (int i = 1; i < args.length; i++) {
        params[i - 1] = Integer.parseInt(args[i]);
      }
    } catch (final NumberFormatException e) {
      return;
    }

    try (final WebCrawler crawler = new WebCrawler(new CachingDownloader(), params[1], params[2], params[3])) {
      crawler.download(args[0], params[0]);
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }
}