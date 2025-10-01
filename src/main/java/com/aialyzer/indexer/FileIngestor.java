//This is just a testing tool. It will be deleted in the final product.
package com.aialyzer.indexer;

import java.nio.file.*;
import java.sql.*;
import java.time.Instant;

public class FileIngestor {

    public static void ingestFolder(Connection cx, Path root) throws Exception {
        cx.setAutoCommit(false);
        long now = Instant.now().getEpochSecond();

            String sql = """
                INSERT INTO files (path, parent_path, size_bytes, mtime_unix, ctime_unix, last_scanned_unix, content_hash, kind, type_label)
                VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?)
                ON CONFLICT(path) DO UPDATE SET
                  size_bytes=excluded.size_bytes,
                  mtime_unix=excluded.mtime_unix,
                  ctime_unix=excluded.ctime_unix,
                  last_scanned_unix=excluded.last_scanned_unix,
                  kind=excluded.kind,
                  type_label=excluded.type_label
            """;

            try (PreparedStatement ps = cx.prepareStatement(sql)) {
                FileScanner.walk(root, (file, attrs) -> {
                    try {
                        String kind = detectKind(file);
                        String typeLabel = ""; 

                        ps.setString(1, file.toAbsolutePath().normalize().toString());
                        ps.setString(2, file.getParent() == null ? "" : file.getParent().toAbsolutePath().normalize().toString());
                        ps.setLong(3, attrs.size());
                        ps.setLong(4, attrs.lastModifiedTime().toMillis() / 1000);
                        ps.setLong(5, attrs.creationTime().toMillis() / 1000);
                        ps.setLong(6, now);
                        ps.setString(7, kind);
                        ps.setString(8, typeLabel);
                        ps.addBatch();
                    } catch (SQLException e) {
                    
                    }
                });

                ps.executeBatch();
            }

            cx.commit();
        }

    private static String detectKind(Path file) {
        String name = file.getFileName() == null ? "" : file.getFileName().toString().toLowerCase();
        int dot = name.lastIndexOf('.');
        String ext = (dot >= 0 && dot < name.length()-1) ? name.substring(dot+1) : "";

        return switch (ext) {
            case "jpg","jpeg","png","gif","bmp","tif","tiff","webp","heic" -> "image";
            case "mp4","mov","mkv","avi","wmv" -> "video";
            case "pdf","doc","docx","xls","xlsx","ppt","pptx","txt","md","csv","json" -> "doc";
            default -> "other";
        };
    }
}
