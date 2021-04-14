package org.taktik.couchdb.entity

data class ChangeEvent(var id: String? = null, var rev: String? = null, var javaType: String? = null, var seq: String? = null)