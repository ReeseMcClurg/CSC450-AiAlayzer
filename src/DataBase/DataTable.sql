-- Master list
CREATE TABLE IF NOT EXISTS files (
  id                INTEGER PRIMARY KEY,
  path              TEXT NOT NULL UNIQUE,
  parent_path       TEXT NOT NULL,
  size_bytes        INTEGER NOT NULL,
  mtime_unix        INTEGER NOT NULL,
  ctime_unix        INTEGER,
  last_scanned_unix INTEGER NOT NULL DEFAULT 0,
  content_hash      TEXT,
  kind              TEXT,      -- image, text, pdf, etc.
  type_label        TEXT,      -- Ai labels in future
  ext               TEXT       -- jpg, txt, pdf, etc.
);

-- Images
CREATE TABLE IF NOT EXISTS image_meta (
  path            TEXT PRIMARY KEY REFERENCES files(path) ON DELETE CASCADE,
  width           INTEGER,
  height          INTEGER,
  exif_taken_unix INTEGER,
  camera_make     TEXT,
  camera_model    TEXT
);

-- History, not used yet
CREATE TABLE IF NOT EXISTS label_history (
  id            INTEGER PRIMARY KEY,
  path          TEXT NOT NULL REFERENCES files(path) ON DELETE CASCADE,
  label         TEXT,
  confidence    REAL,
  source        TEXT,
  created_unix  INTEGER NOT NULL
);

-- Folders
CREATE TABLE IF NOT EXISTS folders (
  path              TEXT PRIMARY KEY,
  last_scanned_unix INTEGER NOT NULL DEFAULT 0,
  priority          INTEGER NOT NULL DEFAULT 0,
  exclude           INTEGER NOT NULL DEFAULT 0
);

-- Queue worker
CREATE TABLE IF NOT EXISTS scan_queue (
  id               INTEGER PRIMARY KEY,
  path             TEXT NOT NULL,
  kind             TEXT NOT NULL,                -- folder, file, image, etc.
  not_before_unix  INTEGER NOT NULL DEFAULT 0,
  attempts         INTEGER NOT NULL DEFAULT 0,
  UNIQUE(path, kind)
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS ix_files_kind_parent_path_path
  ON files(kind, parent_path, path);

CREATE INDEX IF NOT EXISTS ix_files_ext_parent_path_path
  ON files(ext, parent_path, path);

CREATE INDEX IF NOT EXISTS ix_files_parent
  ON files(parent_path);

CREATE INDEX IF NOT EXISTS ix_queue_due
  ON scan_queue(not_before_unix, kind, id);
