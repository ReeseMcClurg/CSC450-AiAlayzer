package com.aialyzer;

import com.aialyzer.indexer.FsIndexer;
import com.aialyzer.queueworker.QueueWorker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class Main {

    private record Config(
    String dbPath,
    boolean passive,
    boolean once,
    List<Path> roots,
    int threads,
    int maxFps
    ) {}

    public static void main(String[] args) {
        Config cfg = parseArgs(args);

        System.out.println("DB: " + cfg.dbPath());
        System.out.println("Passive: " + cfg.passive());
        if (!cfg.roots().isEmpty()) {
            System.out.println("Roots: " + cfg.roots().stream()
                    .map(Path::toString)
                    .collect(Collectors.joining(", ")));
        }

        try {
            Path dbFile = Paths.get(cfg.dbPath()).toAbsolutePath();
            Path dbDir = dbFile.getParent();
            if (dbDir != null) {
                Files.createDirectories(dbDir);
            }
            Class.forName("org.sqlite.JDBC");

            try (Connection cx = com.aialyzer.indexer.DatabaseManager.open(dbFile)) {

                if (!cfg.roots().isEmpty()) {
                    if (!cfg.passive()) {
                        System.out.println("Active full crawl...");
                        new com.aialyzer.indexer.ActiveScanner(cx, cfg.roots(), cfg.threads(), 8192, 800).run();
                        System.out.println("Active crawl complete.");
                    } else {
                        System.out.println("Indexing roots...");
                        new FsIndexer(cx).indexRoots(cfg.roots());
                        System.out.println("Indexing complete.");
                    }
                }

        Connection cxScan = null;
        com.aialyzer.indexer.PassiveScanner passiveScanner = null;
        try {
            if (cfg.passive() && !cfg.once() && !cfg.roots().isEmpty()) {
                cxScan = com.aialyzer.indexer.DatabaseManager.open(dbFile);
                passiveScanner = new com.aialyzer.indexer.PassiveScanner(cxScan, cfg.roots(), cfg.maxFps());
                passiveScanner.startAsync();
                System.out.println("PassiveScanner started (background, max-fps=" + cfg.maxFps() + ").");
            }
        } catch (Exception e) {
            System.out.println("PassiveScanner failed to start; continuing without watcher.");
            e.printStackTrace();
        }

        QueueWorker worker = new QueueWorker(cx, cfg.passive());

        final com.aialyzer.indexer.PassiveScanner psRef = passiveScanner;
        final Connection scanConnRef = cxScan;

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                System.out.println("\nShutting down at " + Instant.now());
                if (psRef != null) psRef.close();
                if (scanConnRef != null && !scanConnRef.isClosed()) scanConnRef.close();
            } catch (Exception ignore) { }
        }));

        if (cfg.once()) {
            worker.runOnce();
            System.out.println("runOnce() complete.");
        } else {
            System.out.println("Starting worker loop at " + Instant.now());
            while (!Thread.currentThread().isInterrupted()) {
                worker.runOnce();
            }
        }
            }
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Config parseArgs(String[] args) {
        String dbPath = "data/app.db";
        boolean passive = true;
        boolean once = false;
        List<Path> roots = new ArrayList<>();

        int threads = Math.max(2, Runtime.getRuntime().availableProcessors());
        int maxFps  = 40; // passive trickle default

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--db" -> {
                    if (i + 1 < args.length) dbPath = args[++i];
                }
                case "--active" -> passive = false;
                case "--passive" -> passive = true;
                case "--once" -> once = true;
                case "--root" -> {
                   if (i + 1 < args.length) roots.add(Paths.get(args[++i]));
                }
                case "--threads" -> {
                    if (i + 1 < args.length) {
                        try {
                            threads = Math.max(1, Integer.parseInt(args[++i]));
                        } catch (NumberFormatException ignore) {}
                    }
                }
                case "--max-fps" -> {
                    if (i + 1 < args.length) {
                        try {
                            maxFps = Math.max(1, Integer.parseInt(args[++i]));
                        } catch (NumberFormatException ignore) {}
                    }
                }
                case "--help", "-h" -> {
                    printHelp();
                    System.exit(0);
                }
                default -> {}
        }
        }

        return new Config(dbPath, passive, once, roots, threads, maxFps);
    }


    private static void printHelp() {
        System.out.println("""
            Usage: java -jar aialyzer.jar [options]

              --db <path>        The file path for SQLite (default: data/app.db)
              --root <dir>       Adds a root to index (can repeat)
              --active           Run worker in active mode (bigger batches)
              --passive          Run worker in passive mode (default, small batch)
              --once             Runs the worker once then exits

              # New (optional):
              --threads <n>      Active mode: number of scan threads (default: CPU cores)
              --max-fps <n>      Passive mode: trickle crawl files/sec budget (default: 40)

              --help             Show help
            """);
    }  
}
