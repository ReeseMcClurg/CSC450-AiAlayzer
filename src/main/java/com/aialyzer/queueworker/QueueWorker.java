package com.aialyzer.queueworker;

import java.sql.*;
import java.time.*; 

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
    // Grab some due tasks
    try (PreparedStatement ps = cx.prepareStatement(
        "select id,path,kind from scan_queue " +
        "where not_before_unix<=? order by kind, id limit ?")) {
      ps.setLong(1, now); ps.setInt(2, batchSize);
      try (ResultSet rs = ps.executeQuery()) {
        boolean didWork = false;
        while (rs.next()) {
          didWork = true;
          int id = rs.getInt(1);
          String path = rs.getString(2);
          String kind = rs.getString(3);
          boolean ok = handle(path, kind);
          if (ok) deleteTask(id); else requeue(id);
        }
        if (!didWork) Thread.sleep(idleSleepMs); 
        cx.commit();
      }
    } catch (Exception e) { cx.rollback(); throw e; }
  }

  private boolean handle(String path, String kind) {
    try {
      switch (kind) {
        case "file" -> { 
            // need a file handler still
        }
        case "image_deep" -> {
          if (passive) { // defers heavy work passive mode
            deferTask(path, "image_deep", Instant.now().getEpochSecond() + 3600);
            return true;
          }
          //Image handler here
        }
        default -> {}
      }
      // pace for passive mode
      if (passive) Thread.sleep(100);
      return true;
    } catch (Exception e) {
      return false;
    }
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

  // test
  public static void main(String[] args) throws Exception {
    try (Connection cx = DriverManager.getConnection("jdbc:sqlite:ai/src/DataBase/app.db")) {
      QueueWorker w = new QueueWorker(cx, true);
      for (int i = 0; i < 1000000; i++) w.runOnce();
    }
  }
}
