/*
 *    Copyright 2020 Taktik SA
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 *
 */

package org.taktik.couchdb.util

import java.lang.reflect.AnnotatedElement
import java.lang.reflect.Field
import java.lang.reflect.Method

fun eachField(clazz: Class<*>, p: Predicate<Field>): Collection<Field> {
    val result = mutableListOf<Field>()

    clazz.declaredFields.forEach {
        if (p.apply(it)) {
            result.add(it)
        }
    }

    if (clazz.superclass != null) {
        result.addAll(eachField(clazz.superclass, p))
    }

    return result
}

fun eachMethod(clazz: Class<*>, p: Predicate<Method>): Collection<Method> {
    val result = mutableListOf<Method>()

    clazz.declaredMethods.forEach {
        if (p.apply(it)) {
            result.add(it)
        }
    }

    if (clazz.superclass != null) {
        result.addAll(eachMethod(clazz.superclass, p))
    }

    return result
}

fun <T : Annotation> eachAnnotation(
        clazz: Class<*>,
        annotationClass: Class<T>, p: Predicate<T>,
) {
    var a = clazz.getAnnotation(annotationClass)

    if (a != null) {
        p.apply(a)
    }

    clazz.declaredMethods.forEach {
        a = it.getAnnotation(annotationClass)

        if (a != null) {
            p.apply(a)
        }
    }

    if (clazz.superclass != null) {
        eachAnnotation(clazz.superclass, annotationClass, p)
    }
}

/**
 * Ignores case when comparing method names
 *
 * @param clazz
 * @param name
 * @return
 */
fun findMethod(clazz: Class<*>, name: String): Method? {
    clazz.declaredMethods.forEach {
        if (it.name.equals(name, ignoreCase = true)) {
            return it
        }
    }

    return if (clazz.superclass != null) {
        findMethod(clazz.superclass, name)
    } else null
}

fun hasAnnotation(e: AnnotatedElement, annotationClass: Class<out Annotation>): Boolean {
    return e.getAnnotation(annotationClass) != null
}

