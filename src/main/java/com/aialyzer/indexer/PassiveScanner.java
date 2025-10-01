package com.aialyzer.indexer;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

public final class PassiveScanner implements AutoCloseable {
  private final Connection cx;
  private final List<Path> roots;
  private final int maxFilesPerSecond;
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private WatchService watcher;
  private volatile boolean stop = false;

  private static final Set<String> EXCLUDE_DIRS = Set.of("$recycle.bin", "node_modules", ".git");
  private static final Set<String> IMAGE_EXT = Set.of("jpg","jpeg","png","gif","bmp","tif","tiff","webp","heic");
  private static final Set<String> VIDEO_EXT = Set.of("mp4","mov","mkv","avi","wmv");
  private static final Set<String> DOC_EXT   = Set.of("pdf","doc","docx","xls","xlsx","ppt","pptx","txt","md","csv","json");

  public PassiveScanner(Connection cx, List<Path> roots, int maxFilesPerSecond) {
    this.cx = cx; this.roots = roots; this.maxFilesPerSecond = Math.max(1, maxFilesPerSecond);
  }

  // trickle crawl
  public void startAsync() throws Exception {
    watcher = FileSystems.getDefault().newWatchService();
    for (Path r : roots) registerAll(r);

    // drain watcher
    scheduler.scheduleWithFixedDelay(this::drainWatcher, 0, 250, TimeUnit.MILLISECONDS);

    // trickle crawl limited below 10Hz
    final int perTick = Math.max(1, maxFilesPerSecond / 10);
    scheduler.scheduleWithFixedDelay(() -> {
      if (stop) return;
      int left = perTick;
      for (Path r : roots) {
        if (left <= 0) break;
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(r)) {
          for (Path p : ds) {
            if (left <= 0) break;
            try {
              BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
              if (a.isRegularFile() && allowedByExt(p)) {
                enqueue(p);
                left--;
              }
            } catch (Exception ignore) {}
          }
        } catch (Exception ignore) {}
      }
    }, 0, 100, TimeUnit.MILLISECONDS);
  }

  private void drainWatcher() {
    try (PreparedStatement ps = cx.prepareStatement(
      "insert into scan_queue(path,kind,not_before_unix,attempts) values (?,?,?,0) " +
      "on conflict(path,kind) do update set not_before_unix=excluded.not_before_unix")) {

      cx.setAutoCommit(false);
      int pending = 0;
      WatchKey key;
      while ((key = watcher.poll()) != null) {
        Path dir = (Path) key.watchable();
        for (WatchEvent<?> ev : key.pollEvents()) {
          var kind = ev.kind();
          if (kind == StandardWatchEventKinds.OVERFLOW) continue;
          Path child = dir.resolve((Path) ev.context());
          try {
            if (Files.isDirectory(child)) {
              if (kind == StandardWatchEventKinds.ENTRY_CREATE) registerAll(child);
              continue;
            }
            if (Files.isRegularFile(child) && allowedByExt(child)) {
              ps.setString(1, child.toString());
              ps.setString(2, "file");
              ps.setLong(3, Instant.now().getEpochSecond());
              ps.addBatch();
              if (++pending >= 400) { ps.executeBatch(); cx.commit(); pending = 0; }
            }
          } catch (Exception ignore) {}
        }
        key.reset();
      }
      if (pending > 0) { ps.executeBatch(); cx.commit(); }
    } catch (Exception ignore) {
    } finally {
      try { cx.setAutoCommit(true); } catch (Exception ignore) {}
    }
  }

  private boolean allowedByExt(Path p) {
    String name = p.getFileName().toString();
    int dot = name.lastIndexOf('.');
    String ext = (dot >= 0 && dot < name.length()-1) ? name.substring(dot+1).toLowerCase() : "";
    return ext.isEmpty() || IMAGE_EXT.contains(ext) || VIDEO_EXT.contains(ext) || DOC_EXT.contains(ext);
  }

  private void enqueue(Path p) {
    long now = Instant.now().getEpochSecond();
    try (PreparedStatement ps = cx.prepareStatement(
      "insert into scan_queue(path,kind,not_before_unix,attempts) values (?,?,?,0) " +
      "on conflict(path,kind) do update set not_before_unix=excluded.not_before_unix")) {
      ps.setString(1, p.toString());
      ps.setString(2, "file");
      ps.setLong(3, now);
      ps.executeUpdate();
    } catch (Exception ignore) {}
  }

  private void registerAll(Path start) throws IOException {
    if (!Files.exists(start)) return;
    Files.walkFileTree(start, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
      @Override public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
        String name = dir.getFileName() == null ? "" : dir.getFileName().toString().toLowerCase();
        if (EXCLUDE_DIRS.contains(name)) return FileVisitResult.SKIP_SUBTREE;
        try {
          dir.register(watcher,
            new WatchEvent.Kind<?>[]{ StandardWatchEventKinds.ENTRY_CREATE,
                                      StandardWatchEventKinds.ENTRY_MODIFY,
                                      StandardWatchEventKinds.ENTRY_DELETE });
        } catch (Exception ignore) {}
        return FileVisitResult.CONTINUE;
      }
    });
  }

  @Override public void close() {
    stop = true;
    scheduler.shutdownNow();
    try { if (watcher != null) watcher.close(); } catch (Exception ignore) {}
  }
}
