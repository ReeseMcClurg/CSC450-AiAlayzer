package com.aialyzer.indexer;

import java.io.IOException;
import java.nio.file.*;
import java.sql.*;

public final class DatabaseManager {
  private DatabaseManager() {}

  public static Connection open(Path dbFile) throws SQLException, IOException {
    Path dir = dbFile.toAbsolutePath().getParent();
    if (dir != null) Files.createDirectories(dir);

    String url = "jdbc:sqlite:" + dbFile.toAbsolutePath();
    Connection cx = DriverManager.getConnection(url);

    try (Statement s = cx.createStatement()) {
      s.execute("PRAGMA foreign_keys=ON;");
      s.execute("PRAGMA journal_mode=WAL;");
      s.execute("PRAGMA synchronous=NORMAL;");
    }
    ensureSchema(cx);
    return cx;
  }

  public static void ensureSchema(Connection cx) throws SQLException {
    try (Statement st = cx.createStatement()) {
      st.execute("PRAGMA busy_timeout=5000;");
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
        );""");

      st.executeUpdate("""
        create table if not exists scan_queue (
          id               integer primary key,
          path             text not null,
          kind             text not null,
          not_before_unix  integer not null,
          attempts         integer not null default 0,
          unique(path, kind)
        );""");

      st.executeUpdate("""
        create table if not exists image_meta (
          path            text primary key references files(path) on delete cascade,
          width           integer,
          height          integer,
          exif_taken_unix integer,
          camera_make     text,
          camera_model    text
        );""");

      st.executeUpdate("""
        create table if not exists label_history (
          id               integer primary key,
          path             text not null,
          label            text,
          confidence       real,
          source           text,
          created_unix     integer not null,
          foreign key(path) references files(path) on delete cascade
        );""");

      st.executeUpdate("""
        create index if not exists ix_files_kind_parent_path_path
        on files(kind, parent_path, path);""");

      st.executeUpdate("""
        create index if not exists ix_files_ext_parent_path_path
        on files(ext, parent_path, path);""");

      st.executeUpdate("""
        create index if not exists ix_queue_due
        on scan_queue(not_before_unix, kind, id);""");

      st.executeUpdate("""
        create index if not exists ix_files_parent
        on files(parent_path);""");
    }
  }
}
