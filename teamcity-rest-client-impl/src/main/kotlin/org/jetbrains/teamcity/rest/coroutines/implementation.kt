package org.jetbrains.teamcity.rest.coroutines

import com.google.gson.Gson
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.take
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import okhttp3.Dispatcher
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.RequestBody.Companion.toRequestBody
import okhttp3.ResponseBody
import okhttp3.logging.HttpLoggingInterceptor
import org.jetbrains.teamcity.rest.*
import org.slf4j.LoggerFactory
import retrofit2.Retrofit
import retrofit2.converter.gson.GsonConverterFactory
import java.io.*
import java.net.HttpURLConnection
import java.net.URI
import java.net.URLEncoder
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.SynchronousQueue
import java.util.concurrent.ThreadFactory
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import javax.xml.stream.XMLOutputFactory
import javax.xml.stream.XMLStreamWriter
import kotlin.concurrent.thread
import kotlin.math.min
import kotlin.math.roundToLong

private val LOG = LoggerFactory.getLogger("teamcity-rest-client")

private val teamCityServiceDateFormat = DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssZ", Locale.ENGLISH)

private class RetryInterceptor(
    private val maxAttempts: Int,
    private val initialDelayMs: Long,
    private val maxDelayMs: Long,
) : Interceptor {
    private val random = Random()
    private val expBackOffFactor: Int = 2
    private val expBackOffJitter: Double = 0.1

    private fun okhttp3.Response.retryRequired(): Boolean {
        val code = code
        if (code < 400) return false

        // Do not retry non-GET methods, their result may be not idempotent
        if (request.method != "GET") return false

        return code == HttpURLConnection.HTTP_CLIENT_TIMEOUT ||
                code == HttpURLConnection.HTTP_INTERNAL_ERROR ||
                code == HttpURLConnection.HTTP_BAD_GATEWAY ||
                code == HttpURLConnection.HTTP_UNAVAILABLE ||
                code == HttpURLConnection.HTTP_GATEWAY_TIMEOUT ||
                code == 429 // Too many requests == rate limited
    }

    override fun intercept(chain: Interceptor.Chain): okhttp3.Response {
        val request = chain.request()

        var error: IOException? = null
        var response: okhttp3.Response? = null

        var nextDelay = initialDelayMs
        for (attempt in 1..maxAttempts) {
            error = null
            response = try {
                chain.proceed(request)
            } catch (e: IOException) {
                error = e
                null
            }

            if (response != null && !response.retryRequired()) {
                return response
            }

            if (response != null) {
                LOG.warn("Request ${request.url} failed: HTTP code ${response.code}, attempt=$attempt, will retry " +
                        "in $nextDelay ms")
            } else {
                LOG.warn("Request ${request.url} failed, attempt=$attempt, will retry in $nextDelay ms", error)
            }
            if (nextDelay != 0L) {
                Thread.sleep(nextDelay)
            }
            val nextRawDelay = minOf(nextDelay * expBackOffFactor, maxDelayMs)
            // (2 * random.nextDouble() - 1.0) -> between -1 and 1
            val jitter = ((2 * random.nextDouble() - 1) * nextRawDelay * expBackOffJitter).roundToLong()
            nextDelay = nextRawDelay + jitter
        }

        if (response != null) {
            return response
        }
        throw TeamCityConversationException("Request ${request.url} failed, tried $maxAttempts times", error)
    }
}

private fun xml(init: XMLStreamWriter.() -> Unit): String {
    val stringWriter = StringWriter()
    val xmlStreamWriter = XMLOutputFactory.newFactory().createXMLStreamWriter(stringWriter)
    init(xmlStreamWriter)
    xmlStreamWriter.flush()
    return stringWriter.toString()
}

private fun XMLStreamWriter.element(name: String, init: XMLStreamWriter.() -> Unit): XMLStreamWriter {
    this.writeStartElement(name)
    this.init()
    this.writeEndElement()
    return this
}

private fun XMLStreamWriter.attribute(name: String, value: String) = writeAttribute(name, value)

private fun selectRestApiCountForPagedRequests(limitResults: Int?, pageSize: Int?): Int? {
    val reasonableMaxPageSize = 1024
    return pageSize ?: limitResults?.let { min(it, reasonableMaxPageSize) }
}

internal class TeamCityCoroutinesInstanceImpl(
    override val serverUrl: String,
    val serverUrlBase: String,
    private val authHeader: String?,
    private val logResponses: Boolean,
    private val timeout: Long,
    private val unit: TimeUnit,
    private val maxConcurrentRequests: Int,
    private val maxConcurrentRequestsPerHost: Int,
    private val retryMaxAttempts: Int,
    private val retryInitialDelayMs: Long,
    private val retryMaxDelayMs: Long,

) : TeamCityCoroutinesInstanceEx {
    override fun toBuilder(): TeamCityInstanceBuilder = TeamCityInstanceBuilder(serverUrl)
        .setUrlBaseAndAuthHeader(serverUrlBase, authHeader)
        .setResponsesLoggingEnabled(logResponses)
        .withTimeout(timeout, unit)
        .withMaxConcurrentRequests(maxConcurrentRequests)
        .withRetry(retryMaxAttempts, retryInitialDelayMs, retryMaxDelayMs, TimeUnit.MILLISECONDS)
        .withMaxConcurrentRequestsPerHost(maxConcurrentRequestsPerHost)

    private val restLog = LoggerFactory.getLogger(LOG.name + ".rest")

    private val loggingInterceptor = HttpLoggingInterceptor { message ->
        restLog.debug(if (authHeader != null) message.replace(authHeader, "[REDACTED]") else message)
    }.apply {
        level = if (logResponses) HttpLoggingInterceptor.Level.BODY else HttpLoggingInterceptor.Level.HEADERS
    }

    private var client = OkHttpClient.Builder()
        .readTimeout(timeout, unit)
        .writeTimeout(timeout, unit)
        .connectTimeout(timeout, unit)
        .addInterceptor { chain ->
            val request = chain.request().newBuilder().apply {
                if (authHeader != null) {
                    header("Authorization", authHeader)
                }
            }.build()
            chain.proceed(request)
        }
        .addInterceptor(loggingInterceptor)
        .addInterceptor(RetryInterceptor(retryMaxAttempts, retryInitialDelayMs, retryMaxDelayMs))
        .dispatcher(Dispatcher(
            //by default non-daemon threads are used, and it blocks JVM from exit
            ThreadPoolExecutor(0, Int.MAX_VALUE, 60, TimeUnit.SECONDS,
                SynchronousQueue(),
                object : ThreadFactory {
                    private val count = AtomicInteger(0)
                    override fun newThread(r: Runnable) = thread(
                        block = { r.run() },
                        isDaemon = true,
                        start = false,
                        name = "TeamCity-Rest-Client - OkHttp Dispatcher - ${count.incrementAndGet()}"
                    )
                }
            ))
            .apply {
                maxRequests = maxConcurrentRequests
                maxRequestsPerHost = maxConcurrentRequestsPerHost
            })
        .build()

    internal val service = Retrofit.Builder()
        .client(client)
        .baseUrl("$serverUrl$serverUrlBase")
        .addConverterFactory(GsonConverterFactory.create())
        .build()
        .create(TeamCityService::class.java)
        .errorCatchingBridge()

    override fun close() {
        fun catchAll(action: () -> Unit): Unit = try {
            action()
        } catch (t: Throwable) {
            LOG.warn("Failed to close connection. ${t.message}", t)
        }

        catchAll { client.dispatcher.cancelAll() }
        catchAll { client.dispatcher.executorService.shutdown() }
        catchAll { client.connectionPool.evictAll() }
        catchAll { client.cache?.close() }
    }

    override fun builds(): BuildLocator = BuildLocatorImpl(this)

    override fun investigations(): InvestigationLocator = InvestigationLocatorImpl(this)

    override fun mutes(): MuteLocator = MuteLocatorImpl(this)

    override fun tests(): TestLocator = TestLocatorImpl(this)
    override suspend fun build(id: BuildId): BuildRef = BuildRefImpl(id, this)

    override suspend fun build(buildConfigurationId: BuildConfigurationId, number: String): BuildRef? =
        BuildLocatorImpl(this).fromConfiguration(buildConfigurationId).withNumber(number).latest()

    override suspend fun buildConfiguration(id: BuildConfigurationId): BuildConfigurationRef =
        BuildConfigurationRefImpl(id, this)

    override fun vcsRoots(): VcsRootLocator = VcsRootLocatorImpl(this)

    override suspend fun vcsRoot(id: VcsRootId): VcsRootRef = VcsRootImpl(service.vcsRoot(id.stringId), true, this)

    override suspend fun project(id: ProjectId): ProjectRef = ProjectRefImpl(id,this)

    override suspend fun rootProject(): ProjectRef = project(ProjectId("_Root"))

    override suspend fun user(id: UserId): UserRef = UserRefImpl(id, this)

    override suspend fun user(userName: String): UserRef {
        val bean = service.users("username:$userName")
        return UserImpl(bean, this)
    }

    override fun users(): UserLocator = UserLocatorImpl(this)

    override suspend fun change(buildConfigurationId: BuildConfigurationId, vcsRevision: String): ChangeRef =
        ChangeImpl(service.change(buildType = buildConfigurationId.stringId, version = vcsRevision), this)

    override suspend fun change(id: ChangeId): Change =
        ChangeImpl(ChangeBean().also { it.id = id.stringId }, this)

    override fun buildQueue(): BuildQueue = BuildQueueImpl(this)

    override fun buildAgents(): BuildAgentLocator = BuildAgentLocatorImpl(this)

    override fun buildAgentPools(): BuildAgentPoolLocator = BuildAgentPoolLocatorImpl(this)

    override fun testRuns(): TestRunsLocator = TestRunsLocatorImpl(this)
}

private class BuildAgentLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : BuildAgentLocatorEx {
    private var compatibleConfigurationId: BuildConfigurationId? = null

    override fun compatibleWith(buildConfigurationId: BuildConfigurationId): BuildAgentLocator {
        compatibleConfigurationId = buildConfigurationId
        return this
    }

    private fun getLocator(): String {
        val compatibleConfigurationIdCopy = compatibleConfigurationId
        val parameters = listOfNotNull(
            compatibleConfigurationIdCopy?.let { "compatible:(buildType:(id:${compatibleConfigurationIdCopy.stringId}))" }
        )
        val locator = parameters.joinToString(",")
        LOG.debug("Retrieving agents from ${instance.serverUrl} using query '$locator'")
        return locator
    }

    private suspend fun allAgents(): List<BuildAgentImpl> =
        instance.service.agents().agent.map { BuildAgentImpl(it, false, instance) }

    override fun all(): Flow<BuildAgent> {
        val locator = getLocator()
        return if (locator.isNotEmpty()) {
            lazyPagingFlow(instance,
                getFirstBean = { instance.service.agents(getLocator(), BuildAgentBean.fields) },
                convertToPage = { bean -> Page(bean.agent.map { BuildAgentImpl(it, false, instance) }, bean.nextHref) })
        } else {
            flow { allAgents().forEach { emit(it) } }
        }
    }

    override fun allSeq(): Sequence<BuildAgent> {
        val locator = getLocator()
        return if (locator.isNotEmpty())
            lazyPagingSequence(instance,
                getFirstBean = { instance.service.agents(getLocator(), BuildAgentBean.fields) },
                convertToPage = { bean -> Page(bean.agent.map { BuildAgentImpl(it, false, instance) }, bean.nextHref) })
        else
            runBlocking { allAgents() }.asSequence()
    }
}

private class BuildAgentPoolLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) :
    BuildAgentPoolLocatorEx {
    private suspend fun allPools(): List<BuildAgentPool> = instance.service.agentPools().agentPool
        .map { BuildAgentPoolImpl(it, false, instance) }

    override fun all(): Flow<BuildAgentPool> = flow { allPools().forEach { emit(it) } }

    override fun allSeq(): Sequence<BuildAgentPool> = runBlocking { allPools() }.asSequence()
}

private class UserLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : UserLocatorEx {
    private suspend fun allUsers(): List<UserRef> = instance.service.users().user.map { UserRefImpl(it, instance) }

    override fun all(): Flow<UserRef> = flow { allUsers().forEach { emit(it) } }

    override fun allSeq(): Sequence<UserRef> = runBlocking { allUsers() }.asSequence()
}

private class BuildLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : BuildLocatorEx {
    private var affectedProjectId: ProjectId? = null
    private var buildConfigurationId: BuildConfigurationId? = null
    private var snapshotDependencyTo: BuildId? = null
    private var number: String? = null
    private var vcsRevision: String? = null
    private var since: Instant? = null
    private var until: Instant? = null
    private var status: BuildStatus? = BuildStatus.SUCCESS
    private var tags = ArrayList<String>()
    private var limitResults: Int? = null
    private var pageSize: Int? = null
    private var branch: String? = null
    private var includeAllBranches = false
    private var pinnedOnly = false
    private var personal: String? = null
    private var running: String? = null
    private var canceled: String? = null

    override fun forProject(projectId: ProjectId): BuildLocator {
        this.affectedProjectId = projectId
        return this
    }

    override fun fromConfiguration(buildConfigurationId: BuildConfigurationId): BuildLocatorImpl {
        this.buildConfigurationId = buildConfigurationId
        return this
    }

    override fun snapshotDependencyTo(buildId: BuildId): BuildLocator {
        this.snapshotDependencyTo = buildId
        return this
    }

    override fun withNumber(buildNumber: String): BuildLocator {
        this.number = buildNumber
        return this
    }

    override fun withVcsRevision(vcsRevision: String): BuildLocator {
        this.vcsRevision = vcsRevision
        return this
    }

    override fun includeFailed(): BuildLocator {
        status = null
        return this
    }

    override fun withStatus(status: BuildStatus): BuildLocator {
        this.status = status
        return this
    }

    override fun includeRunning(): BuildLocator {
        running = "any"
        return this
    }

    override fun onlyRunning(): BuildLocator {
        running = "true"
        return this
    }

    override fun includeCanceled(): BuildLocator {
        canceled = "any"
        return this
    }

    override fun onlyCanceled(): BuildLocator {
        canceled = "true"
        return this
    }

    override fun withTag(tag: String): BuildLocator {
        tags.add(tag)
        return this
    }

    override fun withBranch(branch: String): BuildLocator {
        this.branch = branch
        return this
    }

    override fun since(date: Instant): BuildLocator {
        this.since = date
        return this
    }

    override fun until(date: Instant): BuildLocator {
        this.until = date
        return this
    }

    override fun withAllBranches(): BuildLocator {
        if (branch != null) {
            LOG.warn("Branch is ignored because of #withAllBranches")
        }

        this.includeAllBranches = true
        return this
    }

    override fun pinnedOnly(): BuildLocator {
        this.pinnedOnly = true
        return this
    }

    override fun includePersonal(): BuildLocator {
        this.personal = "any"
        return this
    }

    override fun onlyPersonal(): BuildLocator {
        this.personal = "true"
        return this
    }

    override fun limitResults(count: Int): BuildLocator {
        this.limitResults = count
        return this
    }

    override fun pageSize(pageSize: Int): BuildLocator {
        this.pageSize = pageSize
        return this
    }

    override suspend fun latest(): BuildRef? {
        return limitResults(1).all().firstOrNull()
    }

    private fun getLocator(): String {
        val count = selectRestApiCountForPagedRequests(limitResults = limitResults, pageSize = pageSize)

        val parameters = listOfNotNull(
            affectedProjectId?.stringId?.let { "affectedProject:(id:$it)" },
            buildConfigurationId?.stringId?.let { "buildType:$it" },
            snapshotDependencyTo?.stringId?.let { "snapshotDependency:(to:(id:$it))" },
            number?.let { "number:$it" },
            running?.let { "running:$it" },
            canceled?.let { "canceled:$it" },
            vcsRevision?.let { "revision:$it" },
            status?.name?.let { "status:$it" },
            if (tags.isNotEmpty())
                tags.joinToString(",", prefix = "tags:(", postfix = ")")
            else null,
            if (pinnedOnly) "pinned:true" else null,
            count?.let { "count:$it" },

            since?.let { "sinceDate:${teamCityServiceDateFormat.withZone(ZoneOffset.UTC).format(it)}" },
            until?.let { "untilDate:${teamCityServiceDateFormat.withZone(ZoneOffset.UTC).format(it)}" },

            if (!includeAllBranches)
                branch?.let { "branch:$it" }
            else
                "branch:default:any",

            personal?.let { "personal:$it" },

            // Always use default filter since sometimes TC automatically switches between
            // defaultFilter:true and defaultFilter:false
            // See BuildPromotionFinder.java in rest-api, setLocatorDefaults method
            "defaultFilter:true"
        )
        check(parameters.isNotEmpty()) { "At least one parameter should be specified" }
        val locator = parameters.joinToString(",")
        LOG.debug("Retrieving builds from ${instance.serverUrl} using query '$locator'")
        return locator
    }

    override fun all(): Flow<BuildRef> {
        val buildLocator = getLocator()
        val flow = lazyPagingFlow(instance,
            getFirstBean = { instance.service.builds(buildLocator = buildLocator) },
            convertToPage = { buildsBean ->
                Page(data = buildsBean.build.map { BuildRefImpl(it, instance) }, nextHref = buildsBean.nextHref)
            })
        return limitResults?.let(flow::take) ?: flow
    }

    override fun allSeq(): Sequence<BuildRef> {
        val buildLocator = getLocator()
        val sequence = lazyPagingSequence(instance,
            getFirstBean = { instance.service.builds(buildLocator = buildLocator) },
            convertToPage = { buildsBean ->
                Page(data = buildsBean.build.map { BuildRefImpl(it, instance) }, nextHref = buildsBean.nextHref)
            })
        return limitResults?.let(sequence::take) ?: sequence
    }
}

private class InvestigationLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : InvestigationLocatorEx {
    private var limitResults: Int? = null
    private var targetType: InvestigationTargetType? = null
    private var affectedProjectId: ProjectId? = null

    override fun limitResults(count: Int): InvestigationLocator {
        this.limitResults = count
        return this
    }

    override fun withTargetType(targetType: InvestigationTargetType): InvestigationLocator {
        this.targetType = targetType
        return this
    }

    override fun forProject(projectId: ProjectId): InvestigationLocator {
        this.affectedProjectId = projectId
        return this
    }

    private suspend fun getInvestigations(): List<InvestigationImpl> {
        var locator: String? = null

        val parameters = listOfNotNull(
            limitResults?.let { "count:$it" },
            affectedProjectId?.let { "affectedProject:$it" },
            targetType?.let { "type:${it.value}" }
        )

        if (parameters.isNotEmpty()) {
            locator = parameters.joinToString(",")
            LOG.debug("Retrieving investigations from ${instance.serverUrl} using query '$locator'")
        }

        return instance.service
            .investigations(locator)
            .investigation.map { InvestigationImpl(it, true, instance) }
    }

    override fun all(): Flow<Investigation> = flow { getInvestigations().forEach { emit(it) } }

    override fun allSeq(): Sequence<Investigation> = runBlocking { getInvestigations() }.asSequence()
}

private class MuteLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : MuteLocatorEx {
    private var limitResults: Int? = null
    private var reporter: UserId? = null
    private var test: TestId? = null
    private var affectedProjectId: ProjectId? = null

    override fun limitResults(count: Int): MuteLocator {
        this.limitResults = count
        return this
    }

    override fun forProject(projectId: ProjectId): MuteLocator {
        this.affectedProjectId = projectId
        return this
    }

    override fun byUser(userId: UserId): MuteLocator {
        this.reporter = userId
        return this
    }

    override fun forTest(testId: TestId): MuteLocator {
        this.test = testId
        return this
    }

    private suspend fun getMutes(): List<MuteImpl> {
        var muteLocator: String? = null

        val parameters = listOfNotNull(
            limitResults?.let { "count:$it" },
            affectedProjectId?.let { "affectedProject:$it" },
            reporter?.let { "reporter:$it" },
            test?.let { "test:$it" }
        )

        if (parameters.isNotEmpty()) {
            muteLocator = parameters.joinToString(",")
            LOG.debug("Retrieving mutes from ${instance.serverUrl} using query '$muteLocator'")
        }

        return instance.service
            .mutes(muteLocator)
            .mute.map { MuteImpl(it, true, instance) }
    }

    override fun all(): Flow<Mute> = flow {
        getMutes().forEach { emit(it) }
    }

    override fun allSeq(): Sequence<Mute> = runBlocking { getMutes() }.asSequence()
}

private class TestLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : TestLocatorEx {
    private var id: TestId? = null
    private var count: Int? = null
    private var name: String? = null
    private var affectedProjectId: ProjectId? = null
    private var currentlyMuted: Boolean? = null

    override fun limitResults(count: Int): TestLocator {
        this.count = count
        return this
    }

    override fun byId(testId: TestId): TestLocator {
        this.id = testId
        return this
    }

    override fun byName(testName: String): TestLocator {
        this.name = testName
        return this
    }

    override fun currentlyMuted(muted: Boolean): TestLocator {
        this.currentlyMuted = muted
        return this
    }

    override fun forProject(projectId: ProjectId): TestLocator {
        this.affectedProjectId = projectId
        return this
    }

    private suspend fun getTests(): List<TestImpl> {
        require(name != null || id != null || !(affectedProjectId == null || currentlyMuted == null)) {
            "TestLocator needs name or id, or affectedProjectID with e.g. currentlyMuted specified"
        }
        var testLocator: String? = null

        val parameters = listOfNotNull(
            name?.let { "name:$it" },
            id?.let { "id:$it" },
            affectedProjectId?.let { "affectedProject:$it" },
            currentlyMuted?.let { "currentlyMuted:$it" },
            count?.let { "count:$it" }
        )

        if (parameters.isNotEmpty()) {
            testLocator = parameters.joinToString(",")
            LOG.debug("Retrieving test from ${instance.serverUrl} using query '$testLocator'")
        }

        return instance.service
            .tests(testLocator)
            .test.map { TestImpl(it, true, instance) }
    }

    override fun all(): Flow<Test> = flow { getTests().forEach { emit(it) } }

    override fun allSeq(): Sequence<Test> = runBlocking { getTests() }.asSequence()
}

private class TestRunsLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : TestRunsLocatorEx {
    private var limitResults: Int? = null
    private var pageSize: Int? = null
    private var buildId: BuildId? = null
    private var testId: TestId? = null
    private var affectedProjectId: ProjectId? = null
    private var testStatus: TestStatus? = null
    private var expandMultipleInvocations = false
    private var includeDetailsField = true

    override fun limitResults(count: Int): TestRunsLocator {
        this.limitResults = count
        return this
    }

    override fun pageSize(pageSize: Int): TestRunsLocator {
        this.pageSize = pageSize
        return this
    }

    override fun forProject(projectId: ProjectId): TestRunsLocator {
        this.affectedProjectId = projectId
        return this
    }

    override fun forBuild(buildId: BuildId): TestRunsLocator {
        this.buildId = buildId
        return this
    }

    override fun forTest(testId: TestId): TestRunsLocator {
        this.testId = testId
        return this
    }

    override fun withStatus(testStatus: TestStatus): TestRunsLocator {
        this.testStatus = testStatus
        return this
    }

    override fun withoutDetailsField(): TestRunsLocator {
        this.includeDetailsField = false
        return this
    }

    override fun expandMultipleInvocations(): TestRunsLocator {
        this.expandMultipleInvocations = true
        return this
    }

    private data class Locator(val testOccurrencesLocator: String, val fields: String, val isFullBean: Boolean)

    private fun getLocator(): Locator {
        val statusLocator = when (testStatus) {
            null -> null
            TestStatus.FAILED -> "status:FAILURE"
            TestStatus.SUCCESSFUL -> "status:SUCCESS"
            TestStatus.IGNORED -> "ignored:true"
            TestStatus.UNKNOWN -> error("Unsupported filter by test status UNKNOWN")
        }

        val count = selectRestApiCountForPagedRequests(limitResults = limitResults, pageSize = pageSize)
        val parameters = listOfNotNull(
            count?.let { "count:$it" },
            affectedProjectId?.let { "affectedProject:$it" },
            buildId?.let { "build:$it" },
            testId?.let { "test:$it" },
            expandMultipleInvocations.let { "expandInvocations:$it" },
            statusLocator
        )
        require(parameters.isNotEmpty()) { "At least one parameter should be specified" }
        val testOccurrencesLocator = parameters.joinToString(",")


        val fields = TestOccurrenceBean.getFieldsFilter(includeDetailsField)
        val isFullBean = fields == TestOccurrenceBean.fullFieldsFilter
        LOG.debug("Retrieving test occurrences from ${instance.serverUrl} using query '$testOccurrencesLocator'")
        return Locator(testOccurrencesLocator, fields, isFullBean)
    }

    override fun all(): Flow<TestRunRef> {
        val (testOccurrencesLocator, fields, isFullBean) = getLocator()
        val flow = lazyPagingFlow(instance,
            getFirstBean = { instance.service.testOccurrences(testOccurrencesLocator, fields) },
            convertToPage = { testOccurrencesBean ->
                Page(
                    data = testOccurrencesBean.testOccurrence.map { TestRunImpl(it, isFullBean, instance) },
                    nextHref = testOccurrencesBean.nextHref
                )
            })
        return limitResults?.let(flow::take) ?: flow
    }


    override fun allSeq(): Sequence<TestRun> {
        val (testOccurrencesLocator, fields, isFullBean) = getLocator()
        val sequence = lazyPagingSequence(instance,
            getFirstBean = { instance.service.testOccurrences(testOccurrencesLocator, fields) },
            convertToPage = { testOccurrencesBean ->
                Page(
                    data = testOccurrencesBean.testOccurrence.map { TestRunImpl(it, isFullBean, instance) },
                    nextHref = testOccurrencesBean.nextHref
                )
            })
        return limitResults?.let(sequence::take) ?: sequence
    }
}

private abstract class BaseImpl<TBean : IdBean>(
    protected var bean: TBean,
    protected var isFullBean: Boolean,
    protected val instance: TeamCityCoroutinesInstanceImpl
) {
    init {
        if (bean.id == null) {
            throw IllegalStateException("bean.id should not be null")
        }
    }

    protected inline val idString
        get() = bean.id!!

    protected suspend inline fun <T> notnull(getter: (TBean) -> T?): T =
        getter(bean) ?: getter(fullBean.getValue())!!

    protected suspend inline fun <T> nullable(getter: (TBean) -> T?): T? =
        getter(bean) ?: getter(fullBean.getValue())


    val fullBean = SuspendingLazy {
        if (!isFullBean) {
            bean = fetchFullBean()
            isFullBean = true
        }
        bean
    }

    abstract suspend fun fetchFullBean(): TBean
    abstract override fun toString(): String

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        return idString == (other as BaseImpl<*>).idString && instance === other.instance
    }

    override fun hashCode(): Int = idString.hashCode()
}

private abstract class InvestigationMuteBaseImpl<TBean : InvestigationMuteBaseBean>(
    bean: TBean,
    isFullProjectBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    BaseImpl<TBean>(bean, isFullProjectBean, instance) {
    val id: InvestigationId = InvestigationId(idString)

    val reporter: UserRef? by lazy {
        bean.assignment?.user?.let { userBean -> UserRefImpl(userBean, instance) }
    }

    val comment: String = bean.assignment?.text ?: ""
    val resolveMethod: InvestigationResolveMethod by lazy {
        when (bean.resolution?.type) {
            "whenFixed" -> InvestigationResolveMethod.WHEN_FIXED
            "manually" -> InvestigationResolveMethod.MANUALLY
            else -> error("Properties are invalid")
        }
    }

    val targetType: InvestigationTargetType by lazy {
        val target = checkNotNull(bean.target)
        when {
            target.tests != null -> InvestigationTargetType.TEST
            target.problems != null -> InvestigationTargetType.BUILD_PROBLEM
            else -> InvestigationTargetType.BUILD_CONFIGURATION
        }
    }

    val testIds: List<TestId>? by lazy { bean.target?.tests?.test?.map { x -> TestId(x.id!!) } }

    val problemIds: List<BuildProblemId>? by lazy {
        bean.target?.problems?.problem?.map { x -> BuildProblemId(x.id!!) }
    }

    val scope: InvestigationScope by lazy {
            val scope = bean.scope!!
            val project = scope.project?.let { bean -> ProjectRefImpl(bean, instance) }
            if (project != null) {
                return@lazy InvestigationScope.InProject(project)
            }

            /* neither teamcity.jetbrains nor buildserver contain more then one assignment build type */
            if (scope.buildTypes?.buildType != null && scope.buildTypes.buildType.size > 1) {
                throw IllegalStateException("more then one buildType")
            }
            val buildConfiguration =
                scope.buildTypes?.let { bean -> BuildConfigurationRefImpl(bean.buildType[0], instance) }
            if (buildConfiguration != null) {
                return@lazy InvestigationScope.InBuildConfiguration(buildConfiguration)
            }

            throw IllegalStateException("scope is missed in the bean")
        }
}

private class InvestigationImpl(
    bean: InvestigationBean,
    isFullProjectBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    InvestigationMuteBaseImpl<InvestigationBean>(bean, isFullProjectBean, instance), Investigation {
    override suspend fun fetchFullBean(): InvestigationBean = instance.service.investigation(id.stringId)

    override fun toString(): String = "Investigation(id=$idString,state=$state)"

    override val assignee: UserRef by lazy { UserRefImpl(bean.assignee!!, instance) }

    override val state: InvestigationState by lazy { bean.state!! }
}

private class MuteImpl(
    bean: MuteBean,
    isFullProjectBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    InvestigationMuteBaseImpl<MuteBean>(bean, isFullProjectBean, instance), Mute {

    override val tests: List<TestRef>? by lazy {
        bean.target?.tests?.test?.map { testBean -> TestImpl(testBean, false, instance) }
    }

    override val assignee: UserRef? by lazy {
        bean.assignment?.user?.let { UserRefImpl(it, instance) }
    }

    override suspend fun fetchFullBean(): MuteBean = instance.service.mute(id.stringId)

    override fun toString(): String = "Investigation(id=$idString)"
}

private class ProjectRefImpl(
    override val id: ProjectId,
    private val instance: TeamCityCoroutinesInstanceImpl
) : ProjectRef {
    constructor(stringId: String, instance: TeamCityCoroutinesInstanceImpl) : this(
        id = ProjectId(stringId),
        instance = instance
    )

    constructor(bean: ProjectBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        stringId = checkNotNull(bean.id),
        instance = instance
    )

    override fun getHomeUrl(branch: String?): String =
        getUserUrlPage(instance.serverUrl, "project.html", projectId = id, branch = branch)

    override fun getTestHomeUrl(testId: TestId): String = getUserUrlPage(
        instance.serverUrl, "project.html",
        projectId = id,
        testNameId = testId,
        tab = "testDetails"
    )

    override suspend fun resolve(): Project {
        val bean = instance.service.project(id.stringId)
        return ProjectImpl(bean, instance, this)
    }

    override suspend fun setParameter(name: String, value: String) {
        LOG.info("Setting parameter $name=$value in ProjectId=${id.stringId}")
        instance.service.setProjectParameter(id.stringId, name, value.toRequestBody())
    }

    override suspend fun createProject(id: ProjectId, name: String): Project {
        val projectXmlDescription = xml {
            element("newProjectDescription") {
                attribute("name", name)
                attribute("id", id.stringId)
                element("parentProject") {
                    attribute("locator", "id:${this@ProjectRefImpl.id.stringId}")
                }
            }
        }

        val bean = instance.service.createProject(projectXmlDescription.toRequestBody())
        return ProjectImpl(bean, instance)
    }

    override suspend fun createVcsRoot(
        id: VcsRootId,
        name: String,
        type: VcsRootType,
        properties: Map<String, String>
    ): VcsRoot {
        val vcsRootDescriptionXml = xml {
            element("vcs-root") {
                attribute("name", name)
                attribute("id", id.stringId)
                attribute("vcsName", type.stringType)

                element("project") {
                    attribute("id", this@ProjectRefImpl.id.stringId)
                }

                element("properties") {
                    properties.entries.sortedBy { it.key }.forEach {
                        element("property") {
                            attribute("name", it.key)
                            attribute("value", it.value)
                        }
                    }
                }
            }
        }

        val vcsRootBean = instance.service.createVcsRoot(vcsRootDescriptionXml.toRequestBody())
        return VcsRootImpl(vcsRootBean, true, instance)
    }

    override suspend fun createBuildConfiguration(buildConfigurationDescriptionXml: String): BuildConfigurationRef {
        val bean = instance.service.createBuildType(buildConfigurationDescriptionXml.toRequestBody())
        return BuildConfigurationRefImpl(bean, instance)
    }

    override fun toString(): String = "ProjectRef(id=$id)"
}

private class ProjectImpl(
    bean: ProjectBean,
    instance: TeamCityCoroutinesInstanceImpl,
    ref: ProjectRef = ProjectRefImpl(bean, instance)
) : Project, ProjectRef by ref {
    init {
        check(ref.id.stringId == bean.id)
    }

    override val name: String = checkNotNull(bean.name)
    override val archived: Boolean = bean.archived ?: false
    override val parameters: List<Parameter> = checkNotNull(bean.parameters?.property).map { ParameterImpl(it) }

    override val parentProject: ProjectRef? = bean.parentProjectId?.let { ProjectRefImpl(it, instance) }

    override val childProjects: List<ProjectRef> = checkNotNull(bean.projects).project.map {
        ProjectRefImpl(ProjectId(checkNotNull(it.id)), instance)
    }

    override val buildConfigurations: List<BuildConfigurationRef> = checkNotNull(bean.buildTypes).buildType.map {
        BuildConfigurationRefImpl(it, instance)
    }

    override fun toString(): String = "Project(id='$id',name='$name', archived=$archived, parentProject=$parentProject)"
}

private class BuildConfigurationRefImpl(
    override val id: BuildConfigurationId,
    private val instance: TeamCityCoroutinesInstanceImpl,
) : BuildConfigurationRef {
    constructor(bean: BuildTypeBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        id = BuildConfigurationId(checkNotNull(bean.id)),
        instance = instance
    )

    override fun toString(): String = "BuildConfigurationRef(id=$id)"

    override suspend fun resolve(): BuildConfiguration {
        val bean = instance.service.buildConfiguration(id.stringId)
        return BuildConfigurationImpl(bean, instance, this)
    }

    override fun getHomeUrl(branch: String?): String = getUserUrlPage(
        instance.serverUrl, "viewType.html", buildTypeId = id, branch = branch
    )

    override suspend fun getBuildTags(): List<String> {
        return instance.service.buildTypeTags(id.stringId).tag!!.map { it.name!! }
    }

    override suspend fun getFinishBuildTriggers(): List<FinishBuildTrigger> {
        return instance.service.buildTypeTriggers(id.stringId)
            .trigger
            ?.filter { it.type == "buildDependencyTrigger" }
            ?.map { FinishBuildTriggerImpl(it) }.orEmpty()
    }

    override suspend fun getArtifactDependencies(): List<ArtifactDependency> {
        return instance.service
            .buildTypeArtifactDependencies(id.stringId)
            .`artifact-dependency`
            ?.filter { it.disabled == false }
            ?.map { ArtifactDependencyImpl(it, instance) }.orEmpty()
    }

    override suspend fun setBuildCounter(value: Int) {
        LOG.info("Setting build counter to '$value' in BuildConfigurationId=${id.stringId}")
        instance.service.setBuildTypeSettings(id.stringId, "buildNumberCounter", value.toString().toRequestBody())
    }

    override suspend fun setBuildNumberFormat(format: String) {
        LOG.info("Setting build number format to '$format' in BuildConfigurationId=${id.stringId}")
        instance.service.setBuildTypeSettings(id.stringId, "buildNumberPattern", format.toRequestBody())
    }

    override suspend fun setParameter(name: String, value: String) {
        LOG.info("Setting parameter $name=$value in BuildConfigurationId=${id.stringId}")
        instance.service.setBuildTypeParameter(id.stringId, name, value.toRequestBody())
    }

    override suspend fun runBuild(
        parameters: Map<String, String>?,
        queueAtTop: Boolean,
        cleanSources: Boolean?,
        rebuildAllDependencies: Boolean,
        comment: String?,
        logicalBranchName: String?,
        agentId: String?,
        personal: Boolean,
        revisions: List<SpecifiedRevision>?,
        dependencies: List<BuildId>?
    ): BuildRef {
        val request = TriggerBuildRequestBean()

        request.buildType = BuildTypeBean().apply { id = this@BuildConfigurationRefImpl.id.stringId }
        request.branchName = logicalBranchName
        comment?.let { commentText -> request.comment = CommentBean().apply { text = commentText } }
        request.personal = personal
        request.triggeringOptions = TriggeringOptionsBean().apply {
            this.cleanSources = cleanSources
            this.rebuildAllDependencies = rebuildAllDependencies
            this.queueAtTop = queueAtTop
        }
        parameters?.let { parametersMap ->
            val parametersBean = ParametersBean(parametersMap.map { ParameterBean(it.key, it.value) })
            request.properties = parametersBean
        }
        if (!agentId.isNullOrEmpty())
            request.agent = BuildAgentBean().apply { id = agentId }
        request.`snapshot-dependencies` = dependencies?.let { deps ->
            BuildListBean().apply {
                build = deps.map {
                    BuildBean().apply { id = it.stringId }
                }
            }
        }
        request.revisions = revisions?.let { r ->
            RevisionsBean().apply {
                revision = r.map {
                    RevisionBean().apply {
                        version = it.version
                        vcsBranchName = it.vcsBranchName
                        `vcs-root-instance` = VcsRootInstanceBean().apply {
                            `vcs-root-id` = it.vcsRootId.stringId
                        }
                    }
                }
            }
        }
        val triggeredBuildBean = instance.service.triggerBuild(request)
        return instance.build(BuildId(triggeredBuildBean.id!!.toString()))
    }
}

private class BuildConfigurationImpl(
    bean: BuildTypeBean,
    instance: TeamCityCoroutinesInstanceImpl,
    ref: BuildConfigurationRef = BuildConfigurationRefImpl(bean, instance)
) : BuildConfiguration, BuildConfigurationRef by ref {
    init {
        check(ref.id.stringId == bean.id)
    }

    override val name: String = checkNotNull(bean.name)
    override val projectRef: ProjectRef = ProjectRefImpl(checkNotNull(bean.projectId), instance)
    override val paused: Boolean = bean.paused ?: false

    override val buildCounter: Int = bean.getSetting("buildNumberCounter")?.toIntOrNull()
        ?: throw TeamCityQueryException("Cannot get 'buildNumberCounter' setting for ${id.stringId}")

    override val buildNumberFormat: String = bean.getSetting("buildNumberPattern")
        ?: throw TeamCityQueryException("Cannot get 'buildNumberPattern' setting for ${id.stringId}")

    override fun toString(): String = "Build(id=${id.stringId},name='$name')"

    private fun BuildTypeBean.getSetting(settingName: String): String? =
        settings?.property?.firstOrNull { it.name == settingName }?.value
}

private class VcsRootLocatorImpl(private val instance: TeamCityCoroutinesInstanceImpl) : VcsRootLocatorEx {
    override fun all(): Flow<VcsRoot> = lazyPagingFlow(instance,
        getFirstBean = {
            LOG.debug("Retrieving vcs roots from ${instance.serverUrl}")
            instance.service.vcsRoots()
        },
        convertToPage = { bean -> Page(bean.`vcs-root`.map { VcsRootImpl(it, false, instance) }, bean.nextHref) })

    override fun allSeq(): Sequence<VcsRoot> = lazyPagingSequence(instance,
        getFirstBean = {
            LOG.debug("Retrieving vcs roots from ${instance.serverUrl}")
            instance.service.vcsRoots()
        },
        convertToPage = { bean -> Page(bean.`vcs-root`.map { VcsRootImpl(it, false, instance) }, bean.nextHref) })
}

private class ChangeRefImpl(
    override val id: ChangeId,
    private val instance: TeamCityCoroutinesInstanceImpl,
) : ChangeRef {
    constructor(bean: ChangeBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        id = ChangeId(checkNotNull(bean.id)),
        instance = instance
    )

    override suspend fun resolve(): Change {
        val bean = instance.service.change(changeId = id.stringId)
        return ChangeImpl(bean, instance, this)
    }

    override fun getHomeUrl(
        specificBuildConfigurationId: BuildConfigurationId?,
        includePersonalBuilds: Boolean?
    ): String = getUserUrlPage(
        instance.serverUrl, "viewModification.html",
        modId = id,
        buildTypeId = specificBuildConfigurationId,
        personal = includePersonalBuilds
    )

    override suspend fun firstBuilds(): List<BuildRef> =
        instance.service
            .changeFirstBuilds(id.stringId)
            .build
            .map { BuildRefImpl(it, instance) }

    override suspend fun getFiles(): List<ChangeFile> {
        return instance.service.changeFiles(id.stringId).files?.file?.map { ChangeFileImpl(it) } ?: emptyList()
    }

    override fun toString(): String = "ChangeRef(id=$id)"
}

private class ChangeImpl(
    bean: ChangeBean,
    instance: TeamCityCoroutinesInstanceImpl,
    ref: ChangeRef = ChangeRefImpl(bean, instance)
) : Change, ChangeRef by ref {
    init {
        check(ref.id.stringId == bean.id)
    }

    override val version: String = checkNotNull(bean.version)
    override val username: String = checkNotNull(bean.username)
    override val user: UserRef? = bean.user?.let { userBean -> UserRefImpl(userBean, instance) }
    override val dateTime: ZonedDateTime = ZonedDateTime.parse(checkNotNull(bean.date), teamCityServiceDateFormat)
    override val comment: String = checkNotNull(bean.comment)
    override val vcsRootInstance: VcsRootInstance? = bean.vcsRootInstance?.let(::VcsRootInstanceImpl)

    override fun toString() = "Change(id=$id, version=${version}, username=${username}, user=${user}, " +
            "date=${dateTime}, comment=${comment}, vcsRootInstance=${vcsRootInstance})"
}

private class ChangeFileImpl(private val bean: ChangeFileBean) : ChangeFile {
    override val fileRevisionBeforeChange: String?
        get() = bean.`before-revision`
    override val fileRevisionAfterChange: String?
        get() = bean.`after-revision`
    override val changeType: ChangeType
        get() = try {
            bean.changeType?.let { ChangeType.valueOf(it.uppercase()) } ?: ChangeType.UNKNOWN
        } catch (e: IllegalArgumentException) {
            ChangeType.UNKNOWN
        }
    override val filePath: String?
        get() = bean.file
    override val relativeFilePath: String?
        get() = bean.`relative-file`

    override fun toString(): String {
        return "ChangeFile(fileRevisionBeforeChange=$fileRevisionBeforeChange, fileRevisionAfterChange=$fileRevisionAfterChange, changeType=$changeType, filePath=$filePath, relativeFilePath=$relativeFilePath)"
    }
}

private class UserRefImpl(
    override val id: UserId,
    private val instance: TeamCityCoroutinesInstanceImpl,
) : UserRef {
    constructor(bean: UserBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        id = UserId(checkNotNull(bean.id)),
        instance = instance
    )

    override suspend fun resolve(): User {
        val bean = instance.service.users("id:${id.stringId}")
        return UserImpl(bean, this)
    }

    override fun getHomeUrl(): String = getUserUrlPage(
        instance.serverUrl, "admin/editUser.html",
        userId = id
    )

    override fun toString(): String = "UserRef(id=${id.stringId})"
}

private class UserImpl(
    bean: UserBean,
    ref: UserRef
) : User, UserRef by ref {
    constructor(bean: UserBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        bean = bean,
        ref = UserRefImpl(bean, instance)
    )

    init {
        check(ref.id.stringId == bean.id)
    }

    override val username: String = checkNotNull(bean.username)
    override val name: String? = bean.name
    override val email: String? = bean.name
}

private class PinInfoImpl(bean: PinInfoBean, instance: TeamCityCoroutinesInstanceImpl) : PinInfo {
    override val user by lazy { UserRefImpl(bean.user!!, instance) }
    override val dateTime: ZonedDateTime = ZonedDateTime.parse(bean.timestamp!!, teamCityServiceDateFormat)
}

private class TriggeredImpl(
    private val bean: TriggeredBean,
    private val instance: TeamCityCoroutinesInstanceImpl
) : TriggeredInfo {
    override val user: UserRef? by lazy { bean.user?.let { UserRefImpl(it, instance) } }
    override val build: BuildRef? by lazy { bean.build?.let { BuildRefImpl(it, instance) } }
}

private class BuildCanceledInfoImpl(
    private val bean: BuildCanceledBean,
    private val instance: TeamCityCoroutinesInstanceImpl
) : BuildCanceledInfo {
    override val cancelDateTime: ZonedDateTime by lazy {
        ZonedDateTime.parse(bean.timestamp!!, teamCityServiceDateFormat)
    }
    override val user: UserRef? by lazy { bean.user?.let { UserRefImpl(it, instance) } }
    override val text: String = bean.text ?: ""
}

private class ParameterImpl(private val bean: ParameterBean) : Parameter {
    override val name: String
        get() = bean.name!!

    override val value: String
        get() = bean.value!!

    override val own: Boolean
        get() = bean.own!!
}

private class FinishBuildTriggerImpl(private val bean: TriggerBean) : FinishBuildTrigger {
    override val initiatedBuildConfiguration: BuildConfigurationId
        get() = BuildConfigurationId(bean.properties?.property?.find { it.name == "dependsOn" }?.value!!)

    override val afterSuccessfulBuildOnly: Boolean
        get() = bean.properties?.property?.find { it.name == "afterSuccessfulBuildOnly" }?.value?.toBoolean() ?: false

    private val branchPatterns: List<String>
        get() = bean.properties
            ?.property
            ?.find { it.name == "branchFilter" }
            ?.value
            ?.split(" ").orEmpty()

    override val includedBranchPatterns: Set<String>
        get() = branchPatterns.filter { !it.startsWith("-:") }.mapTo(HashSet()) { it.substringAfter(":") }

    override val excludedBranchPatterns: Set<String>
        get() = branchPatterns.filter { it.startsWith("-:") }.mapTo(HashSet()) { it.substringAfter(":") }
}

private class ArtifactDependencyImpl(
    private val bean: ArtifactDependencyBean,
    private val instance: TeamCityCoroutinesInstanceImpl
) : ArtifactDependency {
    override fun toString(): String = "ArtifactDependency(buildConf=${dependsOnBuildConfiguration.id.stringId})"

    override val dependsOnBuildConfiguration: BuildConfigurationRef
        get() = BuildConfigurationRefImpl(bean.`source-buildType`, instance)

    override val branch: String? by lazy { findPropertyByName("revisionBranch") }

    override val artifactRules: List<ArtifactRule> by lazy {
        checkNotNull(findPropertyByName("pathRules")).split(' ').map(::ArtifactRuleImpl)
    }

    override val cleanDestinationDirectory: Boolean by lazy {
        checkNotNull(findPropertyByName("cleanDestinationDirectory")).toBoolean()
    }

    private fun findPropertyByName(name: String): String? {
        return bean.properties?.property?.find { it.name == name }?.value
    }
}

private class BuildProblemImpl(private val bean: BuildProblemBean) : BuildProblem {
    override val id: BuildProblemId
        get() = BuildProblemId(bean.id!!)
    override val type: BuildProblemType
        get() = BuildProblemType(bean.type!!)
    override val identity: String
        get() = bean.identity!!

    override fun toString(): String =
        "BuildProblem(id=${id.stringId},type=$type,identity=$identity)"
}

private class BuildProblemOccurrenceImpl(
    private val bean: BuildProblemOccurrenceBean,
    private val instance: TeamCityCoroutinesInstanceImpl
) : BuildProblemOccurrence {
    override val buildProblem: BuildProblem
        get() = BuildProblemImpl(bean.problem!!)
    override val build: BuildRef
        get() = BuildRefImpl(bean.build!!, instance)
    override val details: String
        get() = bean.details ?: ""
    override val additionalData: String?
        get() = bean.additionalData

    override fun toString(): String =
        "BuildProblemOccurrence(build=${build.id},problem=$buildProblem,details=$details,additionalData=$additionalData)"
}

internal class ArtifactRuleImpl(private val pathRule: String) : ArtifactRule {
    override val include: Boolean
        get() = !pathRule.startsWith("-:")

    override val sourcePath: String
        get() = pathRule.substringBefore("=>").substringBefore("!").substringAfter(":")

    override val archivePath: String?
        get() = pathRule.substringBefore("=>").substringAfter("!", "").let { if (it != "") it else null }

    override val destinationPath: String?
        get() = pathRule.substringAfter("=>", "").let { if (it != "") it else null }
}

private class RevisionImpl(private val bean: RevisionBean) : Revision {
    override val version: String
        get() = bean.version!!

    override val vcsBranchName: String
        get() = bean.vcsBranchName!!

    override val vcsRootInstance: VcsRootInstance
        get() = VcsRootInstanceImpl(bean.`vcs-root-instance`!!)
}

private data class BranchImpl(
    override val name: String?,
    override val isDefault: Boolean
) : Branch

private data class Page<out T>(val data: List<T>, val nextHref: String?)

private val GSON = Gson()

private inline fun <reified BeanType> ResponseBody.toBean(): BeanType = GSON.fromJson(string(), BeanType::class.java)

private fun String.splitToPathAndParams(): Pair<String, Map<String, String>> {
    val uri = URI(this)
    val path = uri.path
    val fullParams = uri.query
    val paramsMap = fullParams.split("&").associate {
        val (key, value) = it.split("=")
        key to value
    }
    return path to paramsMap
}

private suspend inline fun <reified Bean, T> extractNextPage(
    currentPage: Page<T>,
    instance: TeamCityCoroutinesInstanceImpl,
    crossinline convertToPage: suspend (Bean) -> Page<T>
): Page<T>? {
    val nextHref = currentPage.nextHref ?: return null
    val hrefSuffix = nextHref.removePrefix(instance.serverUrlBase)
    val (path, params) = hrefSuffix.splitToPathAndParams()
    val body = instance.service.root(path, params)
    return convertToPage(body.toBean<Bean>())
}

private inline fun <reified Bean, T> lazyPagingFlow(
    instance: TeamCityCoroutinesInstanceImpl,
    crossinline getFirstBean: suspend () -> Bean,
    crossinline convertToPage: suspend (Bean) -> Page<T>
): Flow<T> = flow {
    var page = convertToPage(getFirstBean())
    while (true) {
        page.data.forEach { emit(it) }
        page = extractNextPage(page, instance, convertToPage) ?: break
    }
}

private inline fun <reified Bean, T> lazyPagingSequence(
    instance: TeamCityCoroutinesInstanceImpl,
    crossinline getFirstBean: suspend () -> Bean,
    crossinline convertToPage: suspend (Bean) -> Page<T>
): Sequence<T> = sequence {
    var page = runBlocking { convertToPage(getFirstBean()) }
    while (true) {
        page.data.forEach { yield(it) }
        page = runBlocking { extractNextPage(page, instance, convertToPage) } ?: break
    }
}

private class BuildRefImpl(
    override val id: BuildId,
    private val instance: TeamCityCoroutinesInstanceImpl
) : BuildRefEx {
    constructor(bean: BuildBean, instance: TeamCityCoroutinesInstanceImpl) : this(
        id = BuildId(checkNotNull(bean.id)),
        instance = instance
    )

    override suspend fun resolve(): Build {
        val bean = instance.service.build(id.stringId)
        return BuildImpl(bean, instance, this)
    }

    override fun getHomeUrl(): String = getUserUrlPage(
        instance.serverUrl, "viewLog.html",
        buildId = id
    )

    override fun getTestRuns(status: TestStatus?): Flow<TestRunRef> = instance
        .testRuns()
        .forBuild(id)
        .let { if (status == null) it else it.withStatus(status) }
        .all()

    override suspend fun getChanges(): List<Change> = instance.service.changes(
        "build:${id.stringId}",
        "change(id,version,username,user,date,comment,vcsRootInstance)"
    ).change!!.map { ChangeImpl(it, instance) }

    override fun getTestRunsSeq(status: TestStatus?): Sequence<TestRunRef> = (instance
            .testRuns()
            .forBuild(id)
            .let { if (status == null) it else it.withStatus(status) } as TestRunsLocatorEx)
            .allSeq()

    override fun getBuildProblems(): Flow<BuildProblemOccurrence> = lazyPagingFlow(instance,
        getFirstBean = {
            instance.service.problemOccurrences(
                locator = "build:(id:${id.stringId})",
                fields = "\$long,problemOccurrence(\$long)"
            )
        }, convertToPage = { bean ->
            Page(
                data = bean.problemOccurrence.map { BuildProblemOccurrenceImpl(it, instance) },
                nextHref = bean.nextHref
            )
        })

    override fun getBuildProblemsSeq(): Sequence<BuildProblemOccurrence> = lazyPagingSequence(instance,
        getFirstBean = {
            instance.service.problemOccurrences(
                locator = "build:(id:${id.stringId})",
                fields = "\$long,problemOccurrence(\$long)"
            )
        }, convertToPage = { bean ->
            Page(
                data = bean.problemOccurrence.map { BuildProblemOccurrenceImpl(it, instance) },
                nextHref = bean.nextHref
            )
        })

    override suspend fun addTag(tag: String) {
        LOG.info("Adding tag $tag to build ${getHomeUrl()}")
        instance.service.addTag(id.stringId, tag.toRequestBody())
    }

    override suspend fun setComment(comment: String) {
        LOG.info("Adding comment $comment to build ${getHomeUrl()}")
        instance.service.setComment(id.stringId, comment.toRequestBody())
    }

    override suspend fun replaceTags(tags: List<String>) {
        LOG.info("Replacing tags of build ${getHomeUrl()} with ${tags.joinToString(", ")}")
        val tagBeans = tags.map { tag -> TagBean().apply { name = tag } }
        instance.service.replaceTags(id.stringId, TagsBean().apply { tag = tagBeans })
    }

    override suspend fun pin(comment: String) {
        LOG.info("Pinning build ${getHomeUrl()}")
        instance.service.pin(id.stringId, comment.toRequestBody())
    }

    override suspend fun unpin(comment: String) {
        LOG.info("Unpinning build ${getHomeUrl()}")
        instance.service.unpin(id.stringId, comment.toRequestBody())
    }

    override suspend fun getArtifacts(parentPath: String, recursive: Boolean, hidden: Boolean): List<BuildArtifact> {
        val locator = "recursive:$recursive,hidden:$hidden"
        val fields = "file(${ArtifactFileBean.FIELDS})"
        return instance.service.artifactChildren(id.stringId, parentPath, locator, fields).file
            .filter { it.fullName != null && it.modificationTime != null }
            .map {
                BuildArtifactImpl(
                    this,
                    it.name!!,
                    it.fullName!!,
                    it.size,
                    ZonedDateTime.parse(it.modificationTime!!, teamCityServiceDateFormat)
                )
            }
    }

    override suspend fun findArtifact(pattern: String, parentPath: String): BuildArtifact {
        return findArtifact(pattern, parentPath, false)
    }

    override suspend fun findArtifact(pattern: String, parentPath: String, recursive: Boolean): BuildArtifact {
        val list = getArtifacts(parentPath, recursive)
        val regexp = convertToJavaRegexp(pattern)
        val result = list.filter { regexp.matches(it.name) }
        if (result.isEmpty()) {
            val available = list.joinToString(",") { it.name }
            throw TeamCityQueryException("Artifact $pattern not found for buildId=${id.stringId}. Available artifacts: $available.")
        }
        if (result.size > 1) {
            val names = result.joinToString(",") { it.name }
            throw TeamCityQueryException("Several artifacts matching $pattern are found for buildId=${id.stringId}: $names.")
        }
        return result.first()
    }

    override suspend fun downloadArtifacts(pattern: String, outputDir: File) {
        val list = getArtifacts(recursive = true)
        val regexp = convertToJavaRegexp(pattern)
        val matched = list.filter { regexp.matches(it.fullName) }
        if (matched.isEmpty()) {
            val available = list.joinToString(",") { it.fullName }
            throw TeamCityQueryException("No artifacts matching $pattern are found for buildId=${id.stringId}. Available artifacts: $available.")
        }
        outputDir.mkdirs()
        matched.forEach {
            it.download(File(outputDir, it.name))
        }
    }

    override suspend fun openArtifactInputStream(artifactPath: String): InputStream {
        LOG.info("Opening artifact '$artifactPath' stream from build ${getHomeUrl()}")
        return openArtifactInputStreamImpl(artifactPath)
    }

    override suspend fun downloadArtifact(artifactPath: String, output: File) {
        LOG.info("Downloading artifact '$artifactPath' from build ${getHomeUrl()} to $output")
        try {
            output.parentFile?.mkdirs()
            withContext(Dispatchers.IO) {
                FileOutputStream(output).use {
                    downloadArtifactImpl(artifactPath, it)
                }
            }
        } catch (t: Throwable) {
            output.delete()
            throw t
        } finally {
            LOG.debug("Artifact '{}' from build {} downloaded to {}", artifactPath, getHomeUrl(), output)
        }
    }

    override suspend fun downloadArtifact(artifactPath: String, output: OutputStream) {
        LOG.info("Downloading artifact '$artifactPath' from build ${getHomeUrl()}")
        try {
            downloadArtifactImpl(artifactPath, output)
        } finally {
            LOG.debug("Artifact '$artifactPath' from build ${getHomeUrl()} downloaded")
        }
    }

    private suspend fun openArtifactInputStreamImpl(artifactPath: String): InputStream {
        return instance.service.artifactContent(id.stringId, artifactPath).byteStream()
    }

    private suspend fun downloadArtifactImpl(artifactPath: String, output: OutputStream) {
        openArtifactInputStreamImpl(artifactPath).use { input ->
            output.use {
                input.copyTo(output, bufferSize = 512 * 1024)
            }
        }
    }

    override suspend fun downloadBuildLog(output: File) {
        LOG.info("Downloading build log from build ${getHomeUrl()} to $output")

        val response = instance.service.buildLog(id.stringId)
        saveToFile(response, output)

        LOG.debug("Build log from build {} downloaded to {}", getHomeUrl(), output)
    }

    override suspend fun cancel(comment: String, reAddIntoQueue: Boolean) {
        val request = BuildCancelRequestBean()
        request.comment = comment
        request.readdIntoQueue = reAddIntoQueue
        instance.service.cancelBuild(id.stringId, request)
    }

    override suspend fun getResultingParameters(): List<Parameter> {
        return instance.service.resultingProperties(id.stringId).property!!.map { ParameterImpl(it) }
    }

    override suspend fun finish() {
        instance.service.finishBuild(id.stringId)
    }

    override fun toString(): String = "BuildRef(id=$id)"
}

private class BuildImpl(
    bean: BuildBean,
    instance: TeamCityCoroutinesInstanceImpl,
    ref: BuildRefEx,
) : Build, BuildRefEx by ref {
    init {
        check(ref.id.stringId == bean.id)
    }

    override val buildNumber: String? = bean.number
    override val status: BuildStatus? = bean.status

    override val buildConfigurationRef: BuildConfigurationRef =
        BuildConfigurationRefImpl(BuildConfigurationId(checkNotNull(bean.buildTypeId)), instance)


    override val branch: Branch by lazy {
        val branchName = bean.branchName
        val isDefaultBranch = bean.defaultBranch
        BranchImpl(name = branchName, isDefault = isDefaultBranch ?: (branchName == null))
    }

    override val state: BuildState by lazy {
        try {
            val state = checkNotNull(bean.state)
            BuildState.valueOf(state.uppercase())
        } catch (e: IllegalArgumentException) {
            BuildState.UNKNOWN
        }
    }

    override val personal: Boolean = bean.personal ?: false
    override val name: String? = bean.buildType?.name

    override val canceledInfo: BuildCanceledInfo? by lazy {
        bean.canceledInfo?.let { BuildCanceledInfoImpl(it, instance) }
    }
    override val comment: BuildCommentInfo? by lazy { bean.comment?.let { BuildCommentInfoImpl(it, instance) } }
    override val composite: Boolean? = bean.composite
    override val statusText: String? = bean.statusText
    override val queuedDateTime: ZonedDateTime by lazy {
        checkNotNull(bean.queuedDate).let { ZonedDateTime.parse(it, teamCityServiceDateFormat) }
    }
    override val startDateTime: ZonedDateTime? by lazy {
        bean.startDate?.let { ZonedDateTime.parse(it, teamCityServiceDateFormat) }
    }
    override val finishDateTime: ZonedDateTime? by lazy {
        bean.finishDate?.let { ZonedDateTime.parse(it, teamCityServiceDateFormat) }
    }

    override val runningInfo: BuildRunningInfo? by lazy { bean.`running-info`?.let { BuildRunningInfoImpl(it) } }

    override val parameters: List<Parameter> by lazy { bean.properties!!.property!!.map { ParameterImpl(it) } }

    override val tags: List<String> by lazy { bean.tags?.tag?.map { it.name!! } ?: emptyList() }

    override val revisions: List<Revision> by lazy { bean.revisions!!.revision!!.map { RevisionImpl(it) } }

    override val snapshotDependencies: List<BuildRef> by lazy {
        bean.`snapshot-dependencies`?.build?.map { BuildRefImpl(it, instance) } ?: emptyList()
    }

    override val pinInfo: PinInfo? by lazy { bean.pinInfo?.let { PinInfoImpl(it, instance) } }
    override val triggeredInfo: TriggeredInfo? by lazy { bean.triggered?.let { TriggeredImpl(it, instance) } }

    override val agentRef: BuildAgentRef? by lazy {
        val agentBean = bean.agent
        if (agentBean?.id == null) {
            return@lazy null
        }
        BuildAgentImpl(agentBean, false, instance)
    }

    override val detachedFromAgent: Boolean = bean.detachedFromAgent ?: false

    override fun toString() = "Build{id=$id, buildConfigurationId=${buildConfigurationRef.id}, " +
            "buildNumber=${buildNumber}, status=${status}, branch=${branch}}"
}

private class TestImpl(
    bean: TestBean,
    isFullBuildBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    BaseImpl<TestBean>(bean, isFullBuildBean, instance), Test {

    override suspend fun fetchFullBean(): TestBean = instance.service.test(idString)

    override val id: TestId
        get() = TestId(idString)

    override suspend fun getName(): String = notnull { it.name }

    override fun toString(): String =
        if (isFullBean) runBlocking { "Test(id=${id.stringId}, name=${getName()})" }
        else "Test(id=${id.stringId})"
}

private class BuildRunningInfoImpl(private val bean: BuildRunningInfoBean) : BuildRunningInfo {
    override val percentageComplete: Int
        get() = bean.percentageComplete
    override val elapsedSeconds: Long
        get() = bean.elapsedSeconds
    override val estimatedTotalSeconds: Long
        get() = bean.estimatedTotalSeconds
    override val outdated: Boolean
        get() = bean.outdated
    override val probablyHanging: Boolean
        get() = bean.probablyHanging
}

private class BuildCommentInfoImpl(
    private val bean: BuildCommentBean,
    private val instance: TeamCityCoroutinesInstanceImpl
) : BuildCommentInfo {
    override val timestamp: ZonedDateTime
        get() = ZonedDateTime.parse(bean.timestamp!!, teamCityServiceDateFormat)
    override val user: User?
        get() = if (bean.user != null) UserImpl(bean.user!!, false, instance) else null
    override val text: String
        get() = bean.text ?: ""

    override fun toString(): String {
        return "BuildCommentInfo(timestamp=$timestamp,userId=${user?.id},text=$text)"
    }
}

private class VcsRootImpl(
    bean: VcsRootBean,
    isFullVcsRootBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    BaseImpl<VcsRootBean>(bean, isFullVcsRootBean, instance), VcsRoot {

    override suspend fun fetchFullBean(): VcsRootBean = instance.service.vcsRoot(idString)

    override fun toString(): String =
        if (isFullBean) runBlocking { "VcsRoot(id=$id, name=${getName()})" }
        else "VcsRoot(id=$id)"

    override val id: VcsRootId
        get() = VcsRootId(idString)

    override suspend fun getName(): String = notnull { it.name }

    override suspend fun getUrl(): String? = getNameValueProperty(properties.getValue(), "url")

    override suspend fun getDefaultBranch(): String? = getNameValueProperty(properties.getValue(), "branch")

    val properties: SuspendingLazy<List<NameValueProperty>> = SuspendingLazy {
        fullBean.getValue().properties!!.property!!.map { NameValueProperty(it) }
    }
}

private class BuildAgentPoolImpl(
    bean: BuildAgentPoolBean,
    isFullBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    BaseImpl<BuildAgentPoolBean>(bean, isFullBean, instance), BuildAgentPool {

    override suspend fun fetchFullBean(): BuildAgentPoolBean = instance.service.agentPools("id:$idString")

    override fun toString(): String =
        if (isFullBean) runBlocking { "BuildAgentPool(id=$id, name=${getName()})" }
        else "BuildAgentPool(id=$id)"

    override val id: BuildAgentPoolId
        get() = BuildAgentPoolId(idString)

    override suspend fun getName(): String = notnull { it.name }

    override suspend fun getProjects(): List<Project> {
        return fullBean.getValue().projects?.project?.map { ProjectImpl(it, instance) } ?: emptyList()
    }

    override suspend fun getAgents(): List<BuildAgent> {
        return fullBean.getValue().agents?.agent?.map { BuildAgentImpl(it, false, instance) } ?: emptyList()
    }
}

private class BuildAgentImpl(
    bean: BuildAgentBean,
    isFullBean: Boolean,
    instance: TeamCityCoroutinesInstanceImpl
) :
    BaseImpl<BuildAgentBean>(bean, isFullBean, instance), BuildAgent {

    override suspend fun fetchFullBean(): BuildAgentBean = instance.service.agent("id:$idString")

    override fun toString(): String =
        if (isFullBean) runBlocking { "BuildAgent(id=$id, name=${getName()})" }
        else "BuildAgent(id=$id)"

    override val id: BuildAgentId
        get() = BuildAgentId(idString)

    override suspend fun getName(): String = notnull { it.name }

    override suspend fun getPool(): BuildAgentPool = BuildAgentPoolImpl(fullBean.getValue().pool!!, false, instance)

    override suspend fun isConnected(): Boolean = notnull { it.connected }

    override suspend fun isEnabled(): Boolean = notnull { it.enabled }
    override suspend fun isAuthorized(): Boolean = notnull { it.authorized }
    override suspend fun isOutdated(): Boolean = !notnull { it.uptodate }
    override suspend fun getIpAddress(): String = notnull { it.ip }

    override suspend fun getParameters(): List<Parameter> {
        return fullBean.getValue().properties!!.property!!.map { ParameterImpl(it) }
    }

    override suspend fun getEnabledInfo(): BuildAgentEnabledInfo? {
        return fullBean.getValue().enabledInfo?.let { info ->
            info.comment?.let { comment ->
                BuildAgentEnabledInfoImpl(
                    user = comment.user?.let { UserImpl(it, false, instance) },
                    timestamp = ZonedDateTime.parse(comment.timestamp!!, teamCityServiceDateFormat),
                    text = comment.text ?: ""
                )
            }
        }
    }

    override suspend fun getAuthorizedInfo(): BuildAgentAuthorizedInfo? {
        return fullBean.getValue().authorizedInfo?.let { info ->
            info.comment?.let { comment ->
                BuildAgentAuthorizedInfoImpl(
                    user = comment.user?.let { UserImpl(it, false, instance) },
                    timestamp = ZonedDateTime.parse(comment.timestamp!!, teamCityServiceDateFormat),
                    text = comment.text ?: ""
                )
            }
        }
    }

    override suspend fun getCurrentBuild(): Build? {
        return fullBean.getValue().build?.let {
            // API may return an empty build bean, pass it as null
            if (it.id == null) null else BuildImpl(it, false, instance)
        }
    }


    override fun getHomeUrl(): String = "${instance.serverUrl}/agentDetails.html?id=${id.stringId}"

    private class BuildAgentAuthorizedInfoImpl(
        override val user: User?,
        override val timestamp: ZonedDateTime,
        override val text: String
    ) : BuildAgentAuthorizedInfo

    private class BuildAgentEnabledInfoImpl(
        override val user: User?,
        override val timestamp: ZonedDateTime,
        override val text: String
    ) : BuildAgentEnabledInfo
}

private class VcsRootInstanceImpl(private val bean: VcsRootInstanceBean) : VcsRootInstance {
    override val vcsRootId: VcsRootId
        get() = VcsRootId(bean.`vcs-root-id`!!)

    override val name: String
        get() = bean.name!!

    override fun toString(): String {
        return "VcsRootInstanceImpl(id=$vcsRootId, name=$name)"
    }


}

private class NameValueProperty(private val bean: NameValuePropertyBean) {
    val name: String
        get() = bean.name!!

    val value: String?
        get() = bean.value
}

private class BuildArtifactImpl(
    override val build: BuildRef,
    override val name: String,
    override val fullName: String,
    override val size: Long?,
    override val modificationDateTime: ZonedDateTime
) : BuildArtifact {
    override suspend fun download(output: File) {
        build.downloadArtifact(fullName, output)
    }

    override suspend fun download(output: OutputStream) {
        build.downloadArtifact(fullName, output)
    }

    override suspend fun openArtifactInputStream(): InputStream {
        return build.openArtifactInputStream(fullName)
    }
}

private class BuildQueueImpl(private val instance: TeamCityCoroutinesInstanceImpl) : BuildQueueEx {
    override suspend fun removeBuild(id: BuildId, comment: String, reAddIntoQueue: Boolean) {
        val request = BuildCancelRequestBean()
        request.comment = comment
        request.readdIntoQueue = reAddIntoQueue
        instance.service.removeQueuedBuild(id.stringId, request)
    }

    override fun queuedBuilds(projectId: ProjectId?): Flow<Build> {
        val parameters = if (projectId == null) emptyList() else listOf("project:${projectId.stringId}")
        return queuedBuilds(parameters)
    }

    override fun queuedBuilds(buildConfigurationId: BuildConfigurationId): Flow<BuildImpl> {
        val parameters = listOf("buildType:${buildConfigurationId.stringId}")
        return queuedBuilds(parameters)
    }

    override fun queuedBuildsSeq(projectId: ProjectId?): Sequence<Build> {
        val parameters = if (projectId == null) emptyList() else listOf("project:${projectId.stringId}")
        return queuedBuildsSeq(parameters)
    }

    override fun queuedBuildsSeq(buildConfigurationId: BuildConfigurationId): Sequence<Build> {
        val parameters = listOf("buildType:${buildConfigurationId.stringId}")
        return queuedBuildsSeq(parameters)
    }

    private fun queuedBuilds(parameters: List<String>): Flow<BuildRef> = lazyPagingFlow(instance,
        getFirstBean = {
            val buildLocator = if (parameters.isNotEmpty()) parameters.joinToString(",") else null
            LOG.debug("Retrieving queued builds from ${instance.serverUrl} using query '$buildLocator'")
            instance.service.queuedBuilds(locator = buildLocator)
        },
        convertToPage = { buildsBean ->
            Page(
                data = buildsBean.build.map { BuildRefImpl(it, instance) },
                nextHref = buildsBean.nextHref
            )
        })

    private fun queuedBuildsSeq(parameters: List<String>): Sequence<BuildRef> = lazyPagingSequence(instance,
        getFirstBean = {
            val buildLocator = if (parameters.isNotEmpty()) parameters.joinToString(",") else null
            LOG.debug("Retrieving queued builds from ${instance.serverUrl} using query '$buildLocator'")
            instance.service.queuedBuilds(locator = buildLocator)
        },
        convertToPage = { buildsBean ->
            Page(
                data = buildsBean.build.map { BuildRefImpl(it, instance) },
                nextHref = buildsBean.nextHref
            )
        })
}

private fun getNameValueProperty(properties: List<NameValueProperty>, name: String): String? =
    properties.singleOrNull { it.name == name }?.value

private open class TestRunRefImpl(
    override val id: TestRunId,
    private val instance: TeamCityCoroutinesInstanceImpl,
) : TestRunRef {
    override suspend fun resolve(): TestRun {
        val bean = instance.service.testOccurrence(id.stringId, TestOccurrenceBean.fullFieldsFilter)
        TODO("Not yet implemented")
    }
}

private class TestRunImpl(
    bean: TestOccurrenceBean,
    instance: TeamCityCoroutinesInstanceImpl,
    ref: TestRunRef,
) : TestRun, TestRunRef by ref {
    override val name: String = checkNotNull(bean.name)
    override val status: TestStatus by lazy {
        when {
            bean.ignored == true -> TestStatus.IGNORED
            bean.status == "FAILURE" -> TestStatus.FAILED
            bean.status == "SUCCESS" -> TestStatus.SUCCESSFUL
            else -> TestStatus.UNKNOWN
        }
    }
    override val duration: Duration = (bean.duration ?: 0L).let(Duration::ofMillis)
    override val details: String by lazy {
        when (status) {
            TestStatus.IGNORED -> bean.ignoreDetails
            TestStatus.FAILED -> bean.details
            else -> null
        } ?: ""
    }
    override val ignored: Boolean = bean.ignored ?: false
    override val currentlyMuted: Boolean = bean.currentlyMuted ?: false
    override val mutedAtRunningTime: Boolean = bean.muted ?: false
    override val newFailure: Boolean = bean.newFailure ?: false
    override val build: BuildRef by lazy { BuildRefImpl(checkNotNull(bean.build), instance) }
    override val fixedIn: BuildRef? by lazy { bean.nextFixed?.let { BuildRefImpl(it, instance) } }
    override val firstFailedIn: BuildRef? by lazy { bean.firstFailed?.let { BuildRefImpl(it, instance) } }
    override val test: TestRef by lazy { TestRefImpl(checkNotNull(bean.test), instance) }
    override val metadataValues: List<String>? by lazy { bean.metadata?.typedValues?.map { it.value.toString() } }
    override fun toString(): String = "Test(id=${id},name=${name}, status=${status}, duration=${duration}, " +
            "details=${details})"
}

private class TestRefImpl(test: TestBean, instance: TeamCityCoroutinesInstanceImpl) : TestRef

private fun convertToJavaRegexp(pattern: String): Regex {
    return pattern.replace(".", "\\.").replace("*", ".*").replace("?", ".").toRegex()
}

private fun String.urlencode(): String = URLEncoder.encode(this, "UTF-8")

private fun getUserUrlPage(
    serverUrl: String,
    pageName: String,
    tab: String? = null,
    projectId: ProjectId? = null,
    buildId: BuildId? = null,
    testNameId: TestId? = null,
    userId: UserId? = null,
    modId: ChangeId? = null,
    personal: Boolean? = null,
    buildTypeId: BuildConfigurationId? = null,
    branch: String? = null
): String {
    val params = mutableListOf<String>()

    tab?.let { params.add("tab=" + tab.urlencode()) }
    projectId?.let { params.add("projectId=" + projectId.stringId.urlencode()) }
    buildId?.let { params.add("buildId=" + buildId.stringId.urlencode()) }
    testNameId?.let { params.add("testNameId=" + testNameId.stringId.urlencode()) }
    userId?.let { params.add("userId=" + userId.stringId.urlencode()) }
    modId?.let { params.add("modId=" + modId.stringId.urlencode()) }
    personal?.let { params.add("personal=" + if (personal) "true" else "false") }
    buildTypeId?.let { params.add("buildTypeId=" + buildTypeId.stringId.urlencode()) }
    branch?.let { params.add("branch=" + branch.urlencode()) }

    return "$serverUrl/$pageName" +
            if (params.isNotEmpty()) "?${params.joinToString("&")}" else ""
}

private fun saveToFile(body: ResponseBody, file: File) {
    file.parentFile?.mkdirs()
    body.byteStream().use { input ->
        file.outputStream().use { output ->
            input.copyTo(output, bufferSize = 512 * 1024)
        }
    }
}


private class SuspendingLazy<T>(private val producer: suspend () -> T) {
    @Volatile
    private var value: T? = null
    private val mutex = Mutex()

    suspend fun getValue(): T {
        if (value == null) {
            mutex.withLock {
                if (value == null) {
                    value = producer()
                }
            }
        }
        return checkNotNull(value)
    }
}