package com.aialyzer.indexer;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;

public class FileScanner {

    // extension filter
    private static final Set<String> IMAGE_EXT = Set.of("jpg","jpeg","png","gif","bmp","tif","tiff","webp","heic");
    private static final Set<String> VIDEO_EXT = Set.of("mp4","mov","mkv","avi","wmv");
    private static final Set<String> DOC_EXT   = Set.of("pdf","doc","docx","xls","xlsx","ppt","pptx","txt","md","csv","json");

    public interface Visitor {
        void onFile(Path file, BasicFileAttributes attrs) throws IOException;
    }

    public static void walk(Path root, Visitor v) throws IOException {
        Files.walkFileTree(root, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {

            @Override
            public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                String name = dir.getFileName() == null ? "" : dir.getFileName().toString().toLowerCase();
                if (name.equals("$recycle.bin") || name.equals("node_modules") || name.equals(".git")) {
                    return FileVisitResult.SKIP_SUBTREE;
                }
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                if (!attrs.isRegularFile()) return FileVisitResult.CONTINUE;

                String ext = getExt(file.getFileName().toString());
                if (!ext.isEmpty() && !(IMAGE_EXT.contains(ext) || VIDEO_EXT.contains(ext) || DOC_EXT.contains(ext))) {
                    return FileVisitResult.CONTINUE;
                }

                v.onFile(file, attrs);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult visitFileFailed(Path file, IOException exc) {
                return FileVisitResult.CONTINUE;
            }

            private String getExt(String name) {
                int dot = name.lastIndexOf('.');
                return (dot >= 0 && dot < name.length()-1) ? name.substring(dot+1).toLowerCase() : "";
            }
        });
    }
}
