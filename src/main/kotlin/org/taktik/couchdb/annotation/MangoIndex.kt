package org.taktik.couchdb.annotation

@Target(AnnotationTarget.FUNCTION)
@Repeatable
@Retention(AnnotationRetention.RUNTIME)
annotation class MangoIndex(val name: String, val fields: Array<String>)
