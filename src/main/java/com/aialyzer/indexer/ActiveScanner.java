package com.aialyzer.indexer;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.*;

public final class ActiveScanner {
  private final Connection cx;
  private final List<Path> roots;
  private final int threads;
  private final int queueSize;
  private final int batchSize;

  public ActiveScanner(Connection cx, List<Path> roots) {
    this(cx, roots, Math.max(2, Runtime.getRuntime().availableProcessors()), 8192, 800);
  }

  public ActiveScanner(Connection cx, List<Path> roots, int threads, int queueSize, int batchSize) {
    this.cx = cx; this.roots = roots;
    this.threads = threads; this.queueSize = queueSize; this.batchSize = Math.max(100, batchSize);
  }

  // Full crawl 
  public void run() throws Exception {
    final BlockingQueue<Path> q = new ArrayBlockingQueue<>(queueSize);
    final ExecutorService pool = Executors.newFixedThreadPool(threads);
    final long now = Instant.now().getEpochSecond();

    cx.setAutoCommit(false);
    try (PreparedStatement ps = cx.prepareStatement(
      "insert into scan_queue(path,kind,not_before_unix,attempts) values (?,?,?,0) " +
      "on conflict(path,kind) do update set not_before_unix=excluded.not_before_unix")) {

      // batch DB upserts
      final Thread writer = new Thread(() -> {
        int pending = 0;
        try {
          while (true) {
            Path p = q.poll(250, TimeUnit.MILLISECONDS);
            if (p == POISON) break;
            if (p == null) continue;
            ps.setString(1, p.toString());
            ps.setString(2, "file");
            ps.setLong(3, now);
            ps.addBatch();
            if (++pending >= batchSize) { ps.executeBatch(); try { cx.commit(); } catch (Exception ignore) {} pending = 0; }
          }
          if (pending > 0) { ps.executeBatch(); try { cx.commit(); } catch (Exception ignore) {} }
        } catch (Exception ignore) {}
      }, "ActiveScanner-Writer");
      writer.start();

      // walk all roots with FileScanner
      for (Path root : roots) {
        pool.submit(() -> {
          try {
            FileScanner.walk(root, (file, attrs) -> {
              while (true) {
                try { q.put(file); break; } catch (InterruptedException ie) { Thread.currentThread().interrupt(); return; }
              }
            });
          } catch (Exception ignore) {}
        });
      }

      pool.shutdown();
      pool.awaitTermination(365, TimeUnit.DAYS);
      q.offer(POISON);
      writer.join();
    } finally {
      try { cx.setAutoCommit(true); } catch (Exception ignore) {}
    }
  }

  private static final Path POISON = Path.of("_ACTIVE_SCANNER_DONE_");
}
