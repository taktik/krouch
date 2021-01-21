package org.taktik.couchdb.mango

import java.lang.String.format
import java.net.URI

const val MANGO_DESIGN_DOC_NAME_FOR_TYPE = "_design/%s_mango"

data class MangoQuery<T>(
        val fields: List<String>,
        val limit: Int?,
        val selector: Selector?,
        val skip: Int?,
        val use_index: List<String>,
        val bookmark: String?,
        val sort: List<Map<String, String>>?
) {
    fun generateQueryUrlFrom(rootDbPath: String) = URI("$rootDbPath/_find")
}

inline fun <reified T> query(initializer: MangoQueryBuilder<T>.() -> Unit): MangoQuery<T> {
    return MangoQueryBuilder<T>(format(MANGO_DESIGN_DOC_NAME_FOR_TYPE, T::class.java.simpleName)).apply(initializer).build()
}

sealed class MangoOperator()
class EmptyOperator : MangoOperator()
data class EqOperator(val `$eq`: Any) : MangoOperator()
data class GtOperator(val `$gt`: Any) : MangoOperator()
data class GteOperator(val `$gte`: Any) : MangoOperator()
data class LtOperator(val `$lt`: Any) : MangoOperator()
data class LteOperator(val `$lte`: Any) : MangoOperator()
data class ExistOperator(val `$exists`: Boolean) : MangoOperator()
data class ElemMatchOperator(val `$elemMatch` : Map<String, String>) : MangoOperator()
open class Selector
data class AndSelector(val `$and`: List<Map<String, MangoOperator>>?) : Selector()
data class OrSelector(val `$or`: List<Map<String, MangoOperator>>?) : Selector()

class MangoQueryBuilder<T>(val designDocument: String) {
    private lateinit var sortFields: List<String>
    private var conditions = mutableListOf<Condition>()
    private val fields = mutableListOf<String>()
    private lateinit var index: String
    private var limit: Int? = null
    private var skip: Int? = null
    private var bookmark: String? = null
    private lateinit var combination: Combination
    private var cachedQuery: MangoQuery<T>? = null
    private var descending: Boolean = false


    fun fields(vararg fields: String) {
        this.fields.addAll(fields)
    }

    fun index(index: String) {
        this.index = index
    }

    fun build(): MangoQuery<T> {
        if (cachedQuery != null) {
            return cachedQuery!!
        }
        val selectorElements = conditions.map { mapOf(it.field to it.buildMangoOperator()) }
        val selector = when (combination) {
            Combination.AND -> AndSelector(selectorElements)
            Combination.OR -> OrSelector(selectorElements)
        }
        cachedQuery = MangoQuery(
                fields.toList(),
                limit,
                selector,
                skip,
                listOf(designDocument, index),
                bookmark,
                if (descending) {
                    sortFields.map { mapOf(it to "desc") }
                } else {
                    null
                }
        )
        return cachedQuery!!
    }

    fun selector(combination: Combination = Combination.AND, initializer: Condition.() -> Unit) {
        this.combination = combination
        this.conditions = Condition().apply(initializer).conditions
    }

    fun limit(limit: Int) {
        this.limit = limit
    }

    fun skip(skip: Int) {
        this.skip = skip
    }

    fun bookmark(bookmark: String?) {
        this.bookmark = bookmark
    }

    fun sort(descending: Boolean, vararg sortFields: String) {
        this.descending = descending
        this.sortFields = sortFields.asList()
    }

}

enum class Combination { AND, OR }

open class Condition(open var field: String = "") {

    var conditions = mutableListOf<Condition>()

    infix fun String.eq(value: Any?) {
        value?.let { conditions.add(Eq(this, it)) } ?: conditions.add(Exists(this, true))
    }

    infix fun String.gt(value: Any?) {
        value?.let { conditions.add(Gt(this, it)) } ?: conditions.add(Exists(this, true))
    }

    infix fun String.gte(value: Any?) {
        value?.let { conditions.add(Gte(this, it)) } ?: conditions.add(Exists(this, true))
    }

    infix fun String.lt(value: Any?) {
        value?.let { conditions.add(Lt(this, it)) } ?: conditions.add(Exists(this, true))
    }

    infix fun String.lte(value: Any?) {
        value?.let { conditions.add(Lte(this, it)) } ?: conditions.add(Exists(this, true))
    }

    infix fun String.exists(value: Boolean) {
        conditions.add(Exists(this, value))
    }

    infix fun String.elemMatch(value: Map<String, String>?) {
        value?.let { conditions.add(ElemMatch(this, value)) } ?: conditions.add(Exists(this, true))
    }

    open fun buildMangoOperator(): MangoOperator = EmptyOperator()
}

class Eq(override var field: String, private val value: Any) : Condition(field) {
    override fun buildMangoOperator() = EqOperator(value)
}

class Gt(override var field: String, private val value: Any) : Condition(field) {
    override fun buildMangoOperator() = GtOperator(value)
}

class Gte(override var field: String, private val value: Any) : Condition(field) {
    override fun buildMangoOperator() = GteOperator(value)
}

class Lt(override var field: String, private val value: Any) : Condition(field) {
    override fun buildMangoOperator() = LtOperator(value)
}

class Lte(override var field: String, private val value: Any) : Condition(field) {
    override fun buildMangoOperator() = LteOperator(value)
}

class Exists(override var field: String, private val value: Boolean) : Condition(field) {
    override fun buildMangoOperator() = ExistOperator(value)
}

class ElemMatch(override var field: String, private val value: Map<String, String>) : Condition(field) {
    override fun buildMangoOperator() = ElemMatchOperator(value)
}
