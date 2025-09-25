package com.aialyzer.indexer;

import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Instant;
import java.util.EnumSet;
import java.util.List;

public final class FsIndexer {
    private final Connection cx;

    public FsIndexer(Connection cx) {
        this.cx = cx;
    }

    public void indexRoots(List<Path> roots) throws Exception {
        cx.setAutoCommit(false);
        try {
            for (Path root : roots) {
                if (!Files.exists(root)) {
                    continue;
                }

                FileVisitor<Path> visitor = new SimpleFileVisitor<>() {
                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                        if (!attrs.isDirectory()) {
                            try {
                                enqueueFileTask(file);
                            } catch (SQLException ignored) {
                            }
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, java.io.IOException exc) {
                        return FileVisitResult.CONTINUE;
                    }
                };

                Files.walkFileTree(root, EnumSet.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, visitor);
            }
            cx.commit();
        } catch (Exception e) {
            cx.rollback();
            throw e;
        }
    }

    private void enqueueFileTask(Path file) throws SQLException {
        long now = Instant.now().getEpochSecond();
        try (PreparedStatement ps = cx.prepareStatement(
                "insert into scan_queue(path,kind,not_before_unix,attempts) values (?,?,?,0) " +
                "on conflict(path,kind) do update set not_before_unix=excluded.not_before_unix")) {
            ps.setString(1, file.toString());
            ps.setString(2, "file");
            ps.setLong(3, now);
            ps.executeUpdate();
        }
    }
}
