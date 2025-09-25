package com.aialyzer;

import com.aialyzer.indexer.FsIndexer;
import com.aialyzer.queueworker.QueueWorker;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class Main {

    private record Config(String dbPath, boolean passive, boolean once, List<Path> roots) {}

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

            try (Connection cx = DriverManager.getConnection("jdbc:sqlite:" + dbFile)) {
                cx.setAutoCommit(true);

                try (Statement s = cx.createStatement()) {
                    s.execute("PRAGMA foreign_keys=ON;");
                    s.execute("PRAGMA journal_mode=WAL;");
                    s.execute("PRAGMA synchronous=NORMAL;");
                }

                ensureSchema(cx);

                if (!cfg.roots().isEmpty()) {
                    System.out.println("Indexing roots...");
                    new FsIndexer(cx).indexRoots(cfg.roots());
                    System.out.println("Indexing complete.");
                }

                QueueWorker worker = new QueueWorker(cx, cfg.passive());

                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    try {
                        System.out.println("\nShutting down at " + Instant.now());
                        cx.close();
                    } catch (Exception ignore) {
                    }
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

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--db" -> {
                    if (i + 1 < args.length) {
                        dbPath = args[++i];
                    }
                }
                case "--active" -> passive = false;
                case "--passive" -> passive = true;
                case "--once" -> once = true;
                case "--root" -> {
                    if (i + 1 < args.length) {
                        roots.add(Paths.get(args[++i]));
                    }
                }
                case "--help", "-h" -> {
                    printHelp();
                    System.exit(0);
                }
                default -> {
                }
            }
        }

        return new Config(dbPath, passive, once, roots);
    }

    private static void printHelp() {
        System.out.println("""
                Usage: java -jar aialyzer.jar [options]

                  --db <path>        The file path for SQLite (default is: data/app.db)
                  --root <dir>       Adds a root to index (maps file system)
                  --active           Run worker in active mode (bigger batches)
                  --passive          Run worker in passive mode (default, small batch)
                  --once             Runs the worker once then exits
                  --help             Show help
                """);
    }

    private static void ensureSchema(Connection cx) throws SQLException {
        try (Statement st = cx.createStatement()) {
            st.execute("PRAGMA foreign_keys=ON");
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("PRAGMA synchronous=NORMAL");
            st.executeUpdate("""
                    create table if not exists files (
                      id                integer primary key,
                      path              text unique not null,
                      parent_path       text not null,
                      size_bytes        integer not null,
                      mtime_unix        integer not null,
                      ctime_unix        integer,
                      last_scanned_unix integer not null,
                      content_hash      text,
                      kind              text,
                      type_label        text,
                      type_label_confidence real,
                      type_label_source text,
                      type_label_updated_unix integer,
                      ext               text
                    );
                    """);

            st.executeUpdate("""
                    create table if not exists scan_queue (
                      id               integer primary key,
                      path             text not null,
                      kind             text not null,
                      not_before_unix  integer not null,
                      attempts         integer not null default 0,
                      unique(path, kind)
                    );
                    """);

            st.executeUpdate("""
                    create table if not exists image_meta (
                      path            text primary key references files(path) on delete cascade,
                      width           integer,
                      height          integer,
                      exif_taken_unix integer,
                      camera_make     text,
                      camera_model    text
                    );
                    """);

            st.executeUpdate("""
                    create table if not exists label_history (
                      id               integer primary key,
                      path             text not null,
                      label            text,
                      confidence       real,
                      source           text,
                      created_unix     integer not null,
                      foreign key(path) references files(path) on delete cascade
                    );
                    """);

            st.executeUpdate("""
                    create index if not exists ix_files_kind_parent_path_path
                    on files(kind, parent_path, path);
                    """);

            st.executeUpdate("""
                    create index if not exists ix_files_ext_parent_path_path
                    on files(ext, parent_path, path);
                    """);

            st.executeUpdate("""
                    create index if not exists ix_queue_due
                    on scan_queue(not_before_unix, kind, id);
                    """);

            st.executeUpdate("""
                    create index if not exists ix_files_parent
                    on files(parent_path);
                    """);
        }
    }
}
