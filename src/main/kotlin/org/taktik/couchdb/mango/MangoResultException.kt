package org.taktik.couchdb.mango

import org.taktik.couchdb.exception.DbAccessException

class MangoResultException(error: String?, reason: String?) :
    DbAccessException(String.format("error : %s, reason: %s", error, reason))