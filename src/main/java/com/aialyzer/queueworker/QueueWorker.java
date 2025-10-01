package com.aialyzer.queueworker;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.sql.*;
import java.time.*;
import java.util.HexFormat;
import java.awt.image.BufferedImage;
import javax.imageio.ImageIO;

public class QueueWorker {
  private final Connection cx;
  private final boolean passive;
  private final int batchSize;
  private final long idleSleepMs;

  public QueueWorker(Connection cx, boolean passive) throws Exception {
    this.cx = cx;
    this.passive = passive;
    this.batchSize = passive ? 5 : 50;  
    this.idleSleepMs = passive ? 1500 : 100;  
    cx.setAutoCommit(false);
  }

  public void runOnce() throws Exception {
  final long now = Instant.now().getEpochSecond();

  
  boolean prev = cx.getAutoCommit();
  cx.setAutoCommit(true);

  java.util.List<Integer> ids   = new java.util.ArrayList<>();
  java.util.List<String>  paths = new java.util.ArrayList<>();
  java.util.List<String>  kinds = new java.util.ArrayList<>();

  try (PreparedStatement ps = cx.prepareStatement(
      "select id,path,kind from scan_queue " +
      "where not_before_unix<=? order by not_before_unix, kind, id limit ?")) {
    ps.setLong(1, now);
    ps.setInt(2, batchSize);
    try (ResultSet rs = ps.executeQuery()) {
      while (rs.next()) {
        ids.add(rs.getInt(1));
        paths.add(rs.getString(2));
        kinds.add(rs.getString(3));
      }
    }
  }

  if (ids.isEmpty()) {
    Thread.sleep(idleSleepMs);
    return;
  }


  cx.setAutoCommit(false);
  try {
    for (int i = 0; i < ids.size(); i++) {
      int id = ids.get(i);
      String path = paths.get(i);
      String kind = kinds.get(i);

      boolean ok = handle(path, kind);
      if (ok) deleteTask(id);
      else    requeue(id);

      if (passive) Thread.sleep(100); 
    }
    cx.commit();
  } catch (Exception e) {
    cx.rollback();
    throw e;
  } finally {
    cx.setAutoCommit(prev); 
  }
}

  private boolean handle(String path, String kind) {
    try {
      switch (kind) {
        case "file" -> {
          handleFile(path);
        }
        case "image_deep" -> {
          if (passive) {                    // defer heavy work in passive mode
            deferTask(path, "image_deep", Instant.now().getEpochSecond() + 3600);
            return true;
          }
          handleImageDeep(path);
        }
        default -> {
      
        }
      }
      if (passive) Thread.sleep(100);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
  
  private static String fileExtLower(Path p) {
  String name = p.getFileName().toString();
  int dot = name.lastIndexOf('.');
  return (dot >= 0 && dot < name.length() - 1) ? name.substring(dot + 1).toLowerCase() : "";
}



  private void handleFile(String pathStr) throws Exception {
    final long now = Instant.now().getEpochSecond();
    Path p = Paths.get(pathStr);
    String parent = (p.getParent() == null) ? "" : p.getParent().toString();

    if (!Files.exists(p)) {
      try (PreparedStatement ps = cx.prepareStatement(
          "insert into files(path,parent_path,size_bytes,mtime_unix,ctime_unix,last_scanned_unix,content_hash,kind,type_label,ext) " +
          "values(?,?,?,?,?,?,?,?,?,?) " +
          "on conflict(path) do update set " +
          "parent_path=excluded.parent_path, " +
          "size_bytes=excluded.size_bytes, " +
          "mtime_unix=excluded.mtime_unix, " +
          "ctime_unix=excluded.ctime_unix, " +
          "last_scanned_unix=excluded.last_scanned_unix, " +
          "kind=excluded.kind, " +
          "type_label=excluded.type_label, " +
          "ext=excluded.ext")) {
        ps.setString(1, pathStr);
        ps.setString(2, parent);
        ps.setLong  (3, 0L);
        ps.setLong  (4, 0L);
        ps.setLong  (5, 0L);
        ps.setLong  (6, now);
        ps.setObject(7, null);
        ps.setString(8, "missing");
        ps.setObject(9, null);
        ps.setObject(10, null);
        ps.executeUpdate();
      }
      return;
    }

    BasicFileAttributes a = Files.readAttributes(p, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
    long size = a.size();
    long mtime = a.lastModifiedTime().toMillis() / 1000L;
    long ctime = a.creationTime() != null ? a.creationTime().toMillis() / 1000L : 0L;
    String mime = safeProbeContentType(p);
    String ext = fileExtLower(p);

    // Store file row
    if (mime == null && "txt".equals(ext)) mime = "text/plain";

    try (PreparedStatement ps = cx.prepareStatement(
         "insert into files(" +
          "  path,parent_path,size_bytes,mtime_unix,ctime_unix,last_scanned_unix,content_hash,kind,type_label,ext" +
          ") values (?,?,?,?,?,?,?,?,?,?) " +
          "on conflict(path) do update set " +
          "  parent_path=excluded.parent_path, " +
          "  size_bytes=excluded.size_bytes, " +
          "  mtime_unix=excluded.mtime_unix, " +
          "  ctime_unix=excluded.ctime_unix, " +
          "  last_scanned_unix=excluded.last_scanned_unix, " +
          "  kind=excluded.kind, " +
          "  type_label=excluded.type_label, " +
          "  ext=excluded.ext"
        )) {
      ps.setString(1, pathStr);
      ps.setString(2, parent);
      ps.setLong  (3, size);
      ps.setLong  (4, mtime);
      ps.setLong  (5, ctime);
      ps.setLong  (6, now);
      ps.setObject(7, null);
      ps.setString(8, mime);
      ps.setObject(9, null);
      ps.setString(10, ext);
      ps.executeUpdate();
    }

    // schedule deeper work when its an image
    if (mime != null && mime.startsWith("image/")) {
      deferTask(pathStr, "image_deep", Instant.now().getEpochSecond());
    }
  }

  private void handleImageDeep(String pathStr) throws Exception {
    final long now = Instant.now().getEpochSecond();
    Path p = Paths.get(pathStr);
    if (!Files.exists(p)) {
      // File vanished, marks file
      try (PreparedStatement ps = cx.prepareStatement(
          "update files set last_scanned_unix=?, kind=? where path=?")) {
        ps.setLong(1, now);
        ps.setString(2, "missing");
        ps.setString(3, pathStr);
        ps.executeUpdate();
      }
      return;
    }

    // Read dimensions
    Integer width = null, height = null;
    try (InputStream in = Files.newInputStream(p)) {
      BufferedImage img = ImageIO.read(in);
      if (img != null) {
        width = img.getWidth();
        height = img.getHeight();
      }
    } catch (IOException ignore) {
    }


    if (width != null && height != null) {
      try (PreparedStatement ps = cx.prepareStatement(
          "insert into image_meta(path,width,height,exif_taken_unix,camera_make,camera_model) " +
          "values (?,?,?,?,?,?) " +
          "on conflict(path) do update set " +
          "  width=excluded.width, " +
          "  height=excluded.height"
      )) {
        ps.setString(1, pathStr);
        ps.setObject(2, width);
        ps.setObject(3, height);
        ps.setObject(4, null);
        ps.setObject(5, null);   // camera_make
        ps.setObject(6, null);   // camera_model
        ps.executeUpdate();
      }
    }


    // caps reading on files larger than 256mb to avoid for passive mode
    String sha256 = null;
    try {
      sha256 = sha256OfFile(p, 256L * 1024 * 1024);
    } catch (IOException ignore) {
    }

    // Update files and store hasg
    try (PreparedStatement ps = cx.prepareStatement(
    "update files set last_scanned_unix=?, content_hash=?, " +
    "type_label=? where path=?")) {
  ps.setLong(1, now);
  if (sha256 == null) ps.setNull(2, Types.VARCHAR);
  else ps.setString(2, sha256);

  if (width != null && height != null) {
    ps.setString(3, width + "x" + height); // store width and height in type_label
  } else {
    ps.setNull(3, Types.VARCHAR);
  }

  ps.setString(4, pathStr);
  ps.executeUpdate();
  }
}

  private static String safeProbeContentType(Path p) {
    try {
      String mime = Files.probeContentType(p);
      // probe returns null, fallback
      if (mime == null) {
        String name = p.getFileName().toString().toLowerCase();
        if (name.endsWith(".jpg") || name.endsWith(".jpeg")) return "image/jpeg";
        if (name.endsWith(".png"))  return "image/png";
        if (name.endsWith(".gif"))  return "image/gif";
        if (name.endsWith(".bmp"))  return "image/bmp";
        if (name.endsWith(".webp")) return "image/webp";
        if (name.endsWith(".heic") || name.endsWith(".heif")) return "image/heic";
        if (name.endsWith(".tif") || name.endsWith(".tiff")) return "image/tiff";
        if (name.endsWith(".mp4"))  return "video/mp4";
        if (name.endsWith(".mov"))  return "video/quicktime";
        if (name.endsWith(".mkv"))  return "video/x-matroska";
        if (name.endsWith(".pdf"))  return "application/pdf";
        if (name.endsWith(".txt"))  return "text/plain";
      }
      return mime;
    } catch (IOException e) {
      return null;
    }
  }

  private static String sha256OfFile(Path p, long maxBytes) throws IOException {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("SHA-256");
    } catch (Exception e) {
      throw new IOException("SHA-256 not available", e);
    }
    long remaining = maxBytes;
    try (InputStream fis = Files.newInputStream(p);
         DigestInputStream dis = new DigestInputStream(fis, md)) {
      byte[] buf = new byte[8192];
      int r;
      while ((r = dis.read(buf, 0, (int)Math.min(buf.length, remaining))) != -1) {
        remaining -= r;
        if (remaining <= 0) break;
      }
    }
    byte[] digest = md.digest();
    return HexFormat.of().formatHex(digest);
  }

  private void deleteTask(int id) throws SQLException {
    try (PreparedStatement ps = cx.prepareStatement("delete from scan_queue where id=?")) {
      ps.setInt(1, id); ps.executeUpdate();
    }
  }

  private void requeue(int id) throws SQLException {
    try (PreparedStatement ps = cx.prepareStatement(
        "update scan_queue set attempts=attempts+1, not_before_unix=? WHERE id=?")) {
      ps.setLong(1, Instant.now().getEpochSecond() + 300); ps.setInt(2, id); ps.executeUpdate();
    }
  }

  private void deferTask(String path, String kind, long notBefore) throws SQLException {
    try (PreparedStatement ps = cx.prepareStatement(
        "insert into scan_queue(path,kind,not_before_unix) values (?,?,?) " +
        "on conflict(path,kind) do update set not_before_unix=excluded.not_before_unix")) {
      ps.setString(1, path); ps.setString(2, kind); ps.setLong(3, notBefore); ps.executeUpdate();
    }
  }
}
