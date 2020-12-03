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

package org.taktik.couchdb.annotation

/**
 * Annotation for defining views embedded in repositories.
 * @author henrik lundgren
 */
@Target(AnnotationTarget.FUNCTION, AnnotationTarget.PROPERTY_GETTER, AnnotationTarget.PROPERTY_SETTER, AnnotationTarget.ANNOTATION_CLASS, AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
annotation class View(
        /**
         * The name of the view
         * @return
         */
        val name: String,
        /**
         * Map function or path to function.
         *
         *
         * This value may be a string of code to use for the function.
         * Alternatively, the string may specify a file to load for
         * the function by starting the string with *classpath:*.
         * The rest of the string then represents a relative path to
         * the function.
         * @return
         */
        val map: String = "",
        /**
         * Reduce function or path to function.
         *
         *
         * This value may be a string of code to use for the function.
         * Alternatively, the string may specify a file to load for
         * the function by starting the string with *classpath:*.
         * The rest of the string then represents a relative path to
         * the function.
         * @return
         */
        val reduce: String = "",
        /**
         * Non-trivial views are best stored in a separate files.
         *
         * By specifying the file parameter a view definition can be loaded from the classpath.
         * The path is relative to the class annotated by this annotation.
         *
         * If the file complicated_view.json is in the same directory as the repository this
         * parameter should be set to "complicated_view.json".
         *
         * The file must be a valid json document:
         *
         * {
         * "map": "function(doc) { much javascript here }",
         * // the reduce function is optional
         * "reduce": "function(keys, values) { ... }"
         * }
         *
         * @return
         */
        val file: String = "")
