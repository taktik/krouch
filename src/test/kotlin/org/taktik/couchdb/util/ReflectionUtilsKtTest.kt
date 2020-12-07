package org.taktik.couchdb.util

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.taktik.couchdb.Code
import org.taktik.couchdb.annotation.View
import org.taktik.couchdb.annotation.Views
import org.taktik.couchdb.entity.Versionable
import java.lang.reflect.Field
import java.lang.reflect.Method

internal class ReflectionUtilsKtTest {

    @Test
    fun eachField() {
        val codeFields = eachField(Code::class.java, object : Predicate<Field> {
            override fun apply(input: Field): Boolean {
                return input.name === "id" || input.name === "deletionDate" || input.name === "attachments" || input.name === "conflicts" || input.name === "javaType" || input.name === "revHistory" || input.name === "rev"
            }
        })

        assertTrue(codeFields.size == 7)
    }

    @Test
    fun eachMethod() {
        val codeMethods = eachMethod(Code::class.java, object : Predicate<Method> {
            override fun apply(input: Method): Boolean {
                return (input.name === "withIdRev" && input.genericReturnType === Code::class.java) ||(input.name === "withIdRev" && input.genericReturnType === Versionable::class.java)
            }
        })

        assertTrue(codeMethods.size == 2)
    }

    @Test
    fun hasAnnotation() {
        assertTrue(hasAnnotation(Code::class.java, Views::class.java))
        assertTrue(hasAnnotation(Code::class.java, View::class.java))
    }
}