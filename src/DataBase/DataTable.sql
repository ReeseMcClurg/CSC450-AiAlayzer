CREATE TABLE IF NOT EXISTS files (
  id                INTEGER PRIMARY KEY,
  path              TEXT NOT NULL UNIQUE,         
  parent_path       TEXT NOT NULL,
  size_bytes        INTEGER NOT NULL,
  mtime_unix        INTEGER NOT NULL,             
  ctime_unix        INTEGER,                      
  last_scanned_unix INTEGER NOT NULL DEFAULT 0,   -- scanner last touched it
  content_hash      TEXT,                         
  kind              TEXT,                         
  type_label        TEXT,                         -- screenshot, text-imag, photo, or doc 
  phash             TEXT,                         
  exif_time_unix    INTEGER,
  exif_gps_lat      REAL,
  exif_gps_lon      REAL,
  ai_reco           INTEGER NOT NULL DEFAULT 0,   -- 0=safe,1=junk,2=suspicious
  flags             INTEGER NOT NULL DEFAULT 0    
);
CREATE INDEX IF NOT EXISTS idx_files_parent ON files(parent_path);
CREATE INDEX IF NOT EXISTS idx_files_mtime  ON files(mtime_unix);
CREATE INDEX IF NOT EXISTS idx_files_kind   ON files(kind, type_label);

-- scheduling
CREATE TABLE IF NOT EXISTS folders (
  path              TEXT PRIMARY KEY,             
  last_scanned_unix INTEGER NOT NULL DEFAULT 0,
  priority          INTEGER NOT NULL DEFAULT 0,
  exclude           INTEGER NOT NULL DEFAULT 0
);

-- Work queue active or passive modes
CREATE TABLE IF NOT EXISTS scan_queue (
  id                INTEGER PRIMARY KEY,
  path              TEXT NOT NULL,                
  kind              TEXT NOT NULL,                -- folder, file, image_deep, ocr
  not_before_unix   INTEGER NOT NULL DEFAULT 0,
  attempts          INTEGER NOT NULL DEFAULT 0,
  UNIQUE(path, kind)
);

-- Duplicate or near duplicate grouping
CREATE TABLE IF NOT EXISTS dup_sets (
  set_id            INTEGER,
  file_id           INTEGER,
  score             REAL,                          
  PRIMARY KEY (set_id, file_id)
);

-- undo
CREATE TABLE IF NOT EXISTS actions_log (
  id        INTEGER PRIMARY KEY,
  ts_unix   INTEGER NOT NULL,
  action    TEXT NOT NULL,
  payload   TEXT NOT NULL,
  undone    INTEGER NOT NULL DEFAULT 0
);

