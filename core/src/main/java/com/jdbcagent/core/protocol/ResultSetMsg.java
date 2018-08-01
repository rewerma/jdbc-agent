package com.jdbcagent.core.protocol;

import javax.sql.RowSet;
import java.io.Serializable;

/**
 * JDBC-Agent protocol ResultSetMsg
 *
 * @author Machengyuan
 * @version 1.0 2018-07-10
 */
public class ResultSetMsg extends Message implements Serializable {
    private static final long serialVersionUID = 148545838290430530L;

    private Long id;
    private Integer batchSize;
    private RowSet rowSet;

    public static Builder newBuilder() {
        return new Builder(new ResultSetMsg());
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public RowSet getRowSet() {
        return rowSet;
    }

    public void setRowSet(RowSet rowSet) {
        this.rowSet = rowSet;
    }

    public Integer getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public static class Builder {
        private ResultSetMsg resultSetMsg;

        public Builder(ResultSetMsg resultSetMsg) {
            this.resultSetMsg = resultSetMsg;
        }

        public Builder setId(Long id) {
            resultSetMsg.setId(id);
            return this;
        }

        public Builder setRowSet(RowSet rowSet) {
            resultSetMsg.setRowSet(rowSet);
            return this;
        }

        public Builder setBatchSize(Integer batchSize) {
            resultSetMsg.setBatchSize(batchSize);
            return this;
        }

        public ResultSetMsg build() {
            return resultSetMsg;
        }
    }
}
