package com.aialyzer.indexer;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;

public class DatabaseManager {

    private static Connection cx;

    public static Connection get() throws SQLException, IOException {
        if (cx != null && !cx.isClosed()) return cx;

        Path dbPath = Paths.get(System.getProperty("user.dir"))
                .resolve("Database")
                .resolve("app.db");
        Files.createDirectories(dbPath.getParent());

        String url = "jdbc:sqlite:" + dbPath.toString();
        cx = DriverManager.getConnection(url);
        try (Statement st = cx.createStatement()) {
            st.execute("PRAGMA foreign_keys=ON");
            st.execute("PRAGMA journal_mode=WAL");
            st.execute("""
                CREATE TABLE IF NOT EXISTS files (
                  id                INTEGER PRIMARY KEY,
                  path              TEXT UNIQUE NOT NULL,
                  parent_path       TEXT NOT NULL,
                  size_bytes        INTEGER NOT NULL,
                  mtime_unix        INTEGER NOT NULL,
                  ctime_unix        INTEGER,
                  last_scanned_unix INTEGER NOT NULL,
                  content_hash      TEXT,
                  kind              TEXT,
                  type_label        TEXT
                )
            """);
            st.execute("CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path)");
        }
        return cx;
    }
}
