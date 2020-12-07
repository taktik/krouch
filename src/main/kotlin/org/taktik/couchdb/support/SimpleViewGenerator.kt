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

package org.taktik.couchdb.support

import com.fasterxml.jackson.databind.ObjectMapper
import org.taktik.couchdb.annotation.View
import org.taktik.couchdb.annotation.Views
import org.taktik.couchdb.util.Exceptions
import org.taktik.couchdb.util.Predicate
import org.taktik.couchdb.util.ReflectionUtils
import java.io.FileNotFoundException
import java.util.HashMap

/**
 *
 * @author Antoine Duchâteau, based on of Ektrop by henrik lundgren
 */
class SimpleViewGenerator {

    fun generateViews(
            repository: Any,
    ): Map<String, org.taktik.couchdb.entity.View> {
        val views: MutableMap<String, org.taktik.couchdb.entity.View> = HashMap()
        val repositoryClass: Class<*> = repository.javaClass
        createDeclaredViews(views, repositoryClass)
        return views
    }

    private fun createDeclaredViews(views: MutableMap<String, org.taktik.couchdb.entity.View>, klass: Class<*>) {
        ReflectionUtils.eachAnnotation(klass, Views::class.java, object : Predicate<Views> {
            override fun apply(input: Views): Boolean {
                for (v in input.value) {
                    addView(views, v, klass)
                }
                return true
            }
        })

        ReflectionUtils.eachAnnotation(klass, View::class.java, object : Predicate<View> {
            override fun apply(input: View): Boolean {
                addView(views, input, klass)
                return true
            }
        })
    }

    private fun addView(
            views: MutableMap<String, org.taktik.couchdb.entity.View>, input: View,
            repositoryClass: Class<*>,
    ) {
        if (input.file.isNotEmpty()) {
            views[input.name] = loadViewFromFile(views, input, repositoryClass)
        } else if (shouldLoadFunctionFromClassPath(input.map)
                || shouldLoadFunctionFromClassPath(input.reduce)) {
            views[input.name] = loadViewFromFile(input, repositoryClass)
        } else {
            views[input.name] = if (input.reduce.isNotEmpty())
                org.taktik.couchdb.entity.View(input.map, input.reduce)
            else org.taktik.couchdb.entity.View(input.map)
        }
    }

    private fun shouldLoadFunctionFromClassPath(function: String?): Boolean {
        return function != null && function.startsWith("classpath:")
    }

    private fun loadViewFromFile(
            input: View,
            repositoryClass: Class<*>,
    ): org.taktik.couchdb.entity.View {
        val mapPath: String = input.map
        val map: String = if (shouldLoadFunctionFromClassPath(mapPath)) {
            loadResourceFromClasspath(repositoryClass, mapPath.substring(10))
        } else {
            mapPath
        }

        val reducePath: String = input.reduce
        val reduce: String? = if (shouldLoadFunctionFromClassPath(reducePath)) {
            loadResourceFromClasspath(repositoryClass, reducePath.substring(10))
        } else {
            if (reducePath.isNotEmpty()) reducePath else null
        }
        return org.taktik.couchdb.entity.View(map, reduce)
    }

    private fun loadResourceFromClasspath(
            repositoryClass: Class<*>,
            path: String,
    ): String {
        return try {
            (repositoryClass.getResourceAsStream(path)
                    ?: throw FileNotFoundException(
                            "Could not load view file with path: $path")).readAllBytes().toString(Charsets.UTF_8)
        } catch (e: Exception) {
            throw Exceptions.propagate(e)
        }
    }

    private fun loadViewFromFile(
            views: Map<String, org.taktik.couchdb.entity.View?>, input: View,
            repositoryClass: Class<*>,
    ): org.taktik.couchdb.entity.View {
        return try {
            val json = loadResourceFromClasspath(repositoryClass,
                    input.file)
            ObjectMapper().readValue(json.replace("\n".toRegex(), ""),
                    org.taktik.couchdb.entity.View::class.java)
        } catch (e: Exception) {
            throw Exceptions.propagate(e)
        }
    }
}
