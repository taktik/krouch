package org.taktik.couchdb.mango;

import org.taktik.couchdb.exception.DbAccessException;

public class MangoResultException extends DbAccessException {
    private String error;
    private String reason;

    public MangoResultException(String error, String reason) {
        super(String.format("error : %s, reason: %s",error, reason));
        this.error = error;
        this.reason = reason;
    }

    public String getError() {
        return error;
    }

    public String getReason() {
        return reason;
    }
}
