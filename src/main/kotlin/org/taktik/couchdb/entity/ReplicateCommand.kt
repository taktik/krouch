package org.taktik.couchdb.entity

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.annotation.JsonProperty
import com.github.pozo.KotlinBuilder
import java.net.URI

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonIgnoreProperties(ignoreUnknown = true)
@KotlinBuilder
data class ReplicateCommand(
        @JsonProperty("_id") val id: String? = null,
        val continuous: Boolean = false,
        @JsonProperty("create_target") val createTarget: Boolean = false,
        @JsonProperty("doc_ids") val docIds: List<String>? = null,
        val cancel: Boolean? = null,
        val filter: String? = null,
        val selector: String? = null,
        val source: Remote,
        val target: Remote
) {
    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonIgnoreProperties(ignoreUnknown = true)
    @KotlinBuilder
    data class Remote(
            val url: String,
            val auth: Authentication? = null
    ) {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        @JsonIgnoreProperties(ignoreUnknown = true)
        @KotlinBuilder
        data class Authentication(
                val basic: Basic? = null
        ) {
            @JsonInclude(JsonInclude.Include.NON_NULL)
            @JsonIgnoreProperties(ignoreUnknown = true)
            @KotlinBuilder
            data class Basic (
                    val username: String,
                    val password: String
                    )
        }
    }

    companion object {
        fun continuous(
                sourceUrl: URI,
                sourceUsername: String,
                sourcePassword: String,
                targetUrl: URI,
                targetUsername: String,
                targetPassword: String,
                id: String? = null
        ) : ReplicateCommand = from (
                sourceUrl,
                sourceUsername,
                sourcePassword,
                targetUrl,
                targetUsername,
                targetPassword,
                createTarget = true,
                continuous = true,
                cancel = null,
                id = id,
                docIds = null,
                filter = null,
                selector = null
        )

        fun oneTime(
                sourceUrl: URI,
                sourceUsername: String,
                sourcePassword: String,
                targetUrl: URI,
                targetUsername: String,
                targetPassword: String,
                id: String? = null
        ) : ReplicateCommand = from (
                sourceUrl,
                sourceUsername,
                sourcePassword,
                targetUrl,
                targetUsername,
                targetPassword,
                createTarget = true,
                continuous = false,
                cancel = null,
                id = id,
                docIds = null,
                filter = null,
                selector = null
        )


        private fun from(
                sourceUrl: URI,
                sourceUsername: String,
                sourcePassword: String,
                targetUrl: URI,
                targetUsername: String,
                targetPassword: String,
                createTarget: Boolean,
                continuous: Boolean,
                cancel: Boolean?,
                id: String?,
                docIds: List<String>?,
                filter: String?,
                selector: String?
        ) : ReplicateCommand = ReplicateCommand (
                    source = Remote(
                            url = sourceUrl.toString(),
                            auth = Remote.Authentication(
                                    basic = Remote.Authentication.Basic(
                                            username = sourceUsername,
                                            password = sourcePassword
                                    )
                            )
                    ),
                    target = Remote(
                            url = targetUrl.toString(),
                            auth = Remote.Authentication(
                                    basic = Remote.Authentication.Basic(
                                            username = targetUsername,
                                            password = targetPassword
                                    )
                            )
                    ),
                    createTarget = createTarget,
                    continuous = continuous,
                    cancel = cancel,
                    id = id,
                    docIds = docIds,
                    filter = filter,
                    selector = selector
            )
    }
}
