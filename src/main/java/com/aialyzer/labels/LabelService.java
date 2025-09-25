package com.aialyzer.labels;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;

public final class LabelService {
    private final Connection cx;

    public LabelService(Connection cx) {
        this.cx = cx;
    }

    public void applyLabel(String path, String label, Double confidence, String source) throws SQLException {
        long now = Instant.now().getEpochSecond();
        cx.setAutoCommit(false);
        try {
            ensureFileExists(path);
            updateFile(path, label, confidence, source, now);
            appendHistory(path, label, confidence, source, now);
            cx.commit();
        } catch (SQLException e) {
            cx.rollback();
            throw e;
        }
    }

    private void ensureFileExists(String path) throws SQLException {
        try (PreparedStatement chk = cx.prepareStatement("select 1 from files where path=?")) {
            chk.setString(1, path);
            try (ResultSet rs = chk.executeQuery()) {
                if (!rs.next()) {
                    throw new SQLException("No such file: " + path);
                }
            }
        }
    }

    private void updateFile(String path, String label, Double confidence, String source, long now) throws SQLException {
        try (PreparedStatement up = cx.prepareStatement(
                "update files set type_label=?, type_label_confidence=?, type_label_source=?, type_label_updated_unix=? where path=?")) {
            if (label == null) {
                up.setNull(1, Types.VARCHAR);
            } else {
                up.setString(1, label);
            }

            if (confidence == null) {
                up.setNull(2, Types.REAL);
            } else {
                up.setDouble(2, confidence);
            }

            if (source == null) {
                up.setNull(3, Types.VARCHAR);
            } else {
                up.setString(3, source);
            }

            up.setLong(4, now);
            up.setString(5, path);
            up.executeUpdate();
        }
    }

    private void appendHistory(String path, String label, Double confidence, String source, long now) throws SQLException {
        try (PreparedStatement ins = cx.prepareStatement(
                "insert into label_history(path,label,confidence,source,created_unix) values (?,?,?,?,?)")) {
            ins.setString(1, path);

            if (label == null) {
                ins.setNull(2, Types.VARCHAR);
            } else {
                ins.setString(2, label);
            }

            if (confidence == null) {
                ins.setNull(3, Types.REAL);
            } else {
                ins.setDouble(3, confidence);
            }

            if (source == null) {
                ins.setNull(4, Types.VARCHAR);
            } else {
                ins.setString(4, source);
            }

            ins.setLong(5, now);
            ins.executeUpdate();
        }
    }
}
