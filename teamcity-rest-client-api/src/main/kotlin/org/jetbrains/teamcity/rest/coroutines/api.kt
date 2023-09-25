package org.jetbrains.teamcity.rest.coroutines

import kotlinx.coroutines.flow.Flow
import org.jetbrains.teamcity.rest.*
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.time.Duration
import java.time.ZonedDateTime

interface TeamCityCoroutinesInstance : AutoCloseable, TeamCityInstanceSettings<TeamCityCoroutinesInstance> {
    fun builds(): BuildLocator
    fun investigations(): InvestigationLocator

    fun mutes(): MuteLocator

    fun tests(): TestLocator
    suspend fun build(id: BuildId): BuildRef
    suspend fun build(buildConfigurationId: BuildConfigurationId, number: String): BuildRef?
    suspend fun buildConfiguration(id: BuildConfigurationId): BuildConfigurationRef
    fun vcsRoots(): VcsRootLocator
    suspend fun vcsRoot(id: VcsRootId): VcsRootRef
    suspend fun project(id: ProjectId): ProjectRef
    suspend fun rootProject(): ProjectRef
    fun buildQueue(): BuildQueue
    suspend fun user(id: UserId): UserRef
    suspend fun user(userName: String): UserRef
    fun users(): UserLocator
    fun buildAgents(): BuildAgentLocator
    fun buildAgentPools(): BuildAgentPoolLocator
    fun testRuns(): TestRunsLocator

    suspend fun change(buildConfigurationId: BuildConfigurationId, vcsRevision: String): ChangeRef
    suspend fun change(id: ChangeId): ChangeRef
}

interface ProjectRef {
    val id: ProjectId
    suspend fun resolve(): Project

    /**
     * Web UI URL for user, especially useful for error and log messages
     */
    fun getHomeUrl(branch: String? = null): String
    fun getTestHomeUrl(testId: TestId): String

    suspend fun setParameter(name: String, value: String)

    /**
     * See properties example from existing VCS roots via inspection of the following url:
     * https://teamcity/app/rest/vcs-roots/id:YourVcsRootId
     */
    suspend fun createVcsRoot(
        id: VcsRootId,
        name: String,
        type: VcsRootType,
        properties: Map<String, String>
    ): VcsRootRef

    suspend fun createProject(id: ProjectId, name: String): Project

    /**
     * XML in the same format as
     * https://teamcity/app/rest/buildTypes/YourBuildConfigurationId
     * returns
     */
    suspend fun createBuildConfiguration(buildConfigurationDescriptionXml: String): BuildConfigurationRef
}

interface BuildRef {
    val id: BuildId
    suspend fun resolve(): Build

    /**
     * Web UI URL for user, especially useful for error and log messages
     */
    fun getHomeUrl(): String

    fun getTestRuns(status: TestStatus? = null): Flow<TestRunRef>

    fun getBuildProblems(): Flow<BuildProblemOccurrence>

    suspend fun addTag(tag: String)
    suspend fun setComment(comment: String)
    suspend fun replaceTags(tags: List<String>)
    suspend fun pin(comment: String = "pinned via REST API")
    suspend fun unpin(comment: String = "unpinned via REST API")
    suspend fun getArtifacts(parentPath: String = "", recursive: Boolean = false, hidden: Boolean = false): List<BuildArtifact>
    suspend fun findArtifact(pattern: String, parentPath: String = ""): BuildArtifact
    suspend fun findArtifact(pattern: String, parentPath: String = "", recursive: Boolean = false): BuildArtifact
    suspend fun downloadArtifacts(pattern: String, outputDir: File)
    suspend fun downloadArtifact(artifactPath: String, output: OutputStream)
    suspend fun downloadArtifact(artifactPath: String, output: File)
    suspend fun openArtifactInputStream(artifactPath: String): InputStream
    suspend fun downloadBuildLog(output: File)
    suspend fun cancel(comment: String = "", reAddIntoQueue: Boolean = false)
    suspend fun getResultingParameters(): List<Parameter>
    suspend fun finish()

    /**
     * Changes is meant to represent changes the same way as displayed in the build's Changes in TeamCity UI.
     * In the most cases these are the commits between the current and previous build.
     */
    suspend fun getChanges(): List<Change>
}

interface TestRef {
    val id: TestId
    suspend fun resolve(): Test
}

interface TestRunRef {
    val id: TestRunId
    suspend fun resolve(): TestRun
}

interface ChangeRef {
    val id: ChangeId
    suspend fun resolve(): Change

    /**
     * Web UI URL for user, especially useful for error and log messages
     */
    fun getHomeUrl(
        specificBuildConfigurationId: BuildConfigurationId? = null,
        includePersonalBuilds: Boolean? = null
    ): String

    /**
     * Returns an uncertain amount of builds which contain the revision. The builds are not necessarily from the same
     * configuration as the revision. The feature is experimental, see https://youtrack.jetbrains.com/issue/TW-24633
     */
    suspend fun firstBuilds(): List<BuildRef>
    suspend fun getFiles(): List<ChangeFile>
}

interface BuildConfigurationRef {
    val id: BuildConfigurationId
    suspend fun resolve(): BuildConfiguration

    /**
     * Web UI URL for user, especially useful for error and log messages
     */
    fun getHomeUrl(branch: String? = null): String

    suspend fun setParameter(name: String, value: String)

    suspend fun setBuildCounter(value: Int)
    suspend fun setBuildNumberFormat(format: String)

    suspend fun runBuild(
        parameters: Map<String, String>? = null,
        queueAtTop: Boolean = false,
        cleanSources: Boolean? = null,
        rebuildAllDependencies: Boolean = false,
        comment: String? = null,
        logicalBranchName: String? = null,
        agentId: String? = null,
        personal: Boolean = false,
        revisions: List<SpecifiedRevision>? = null,
        dependencies: List<BuildId>? = null
    ): BuildRef

    suspend fun getBuildTags(): List<String>
    suspend fun getFinishBuildTriggers(): List<FinishBuildTrigger>
    suspend fun getArtifactDependencies(): List<ArtifactDependency>
}

interface VcsRootRef {
    val id: VcsRootId
    suspend fun resolve(): VcsRoot
}

interface BuildAgentPoolRef {
    val id:  BuildAgentPoolId
    suspend fun resolve(): BuildAgentPool
}

interface BuildAgentRef {
    val id: BuildAgentId
    suspend fun resolve(): BuildAgent
    
    fun getHomeUrl(): String
}

interface UserRef {
    val id: UserId
    suspend fun resolve(): User

    /**
     * Web UI URL for user, especially useful for error and log messages
     */
    fun getHomeUrl(): String
}

interface VcsRootLocator : VcsRootLocatorSettings<VcsRootLocator> {
    fun all(): Flow<VcsRootRef>
}

interface UserLocator : UserLocatorSettings<UserLocator> {
    fun all(): Flow<UserRef>
}

interface BuildAgentLocator : BuildAgentLocatorSettings<BuildAgentLocator> {
    fun all(): Flow<BuildAgentRef>
}

interface BuildAgentPoolLocator {
    fun all(): Flow<BuildAgentPoolRef>
}

interface BuildLocator : BuildLocatorSettings<BuildLocator> {
    suspend fun latest(): BuildRef?
    fun all(): Flow<BuildRef>
}

interface InvestigationLocator : InvestigationLocatorSettings<InvestigationLocator> {
    fun all(): Flow<Investigation>
}

interface MuteLocator : MuteLocatorSettings<MuteLocator> {
    fun all(): Flow<Mute>
}

interface TestLocator : TestLocatorSettings<TestLocator> {
    fun all(): Flow<TestRef>
}

interface TestRunsLocator : TestRunsLocatorSettings<TestRunsLocator> {
    fun all(): Flow<TestRunRef>
}

interface Project : ProjectRef {
    val name: String
    val archived: Boolean
    val parentProject: ProjectRef?

    val childProjects: List<ProjectRef>
    val buildConfigurations: List<BuildConfigurationRef>
    val parameters: List<Parameter>
}

interface BuildConfiguration : BuildConfigurationRef {
    val name: String
    val projectRef: ProjectRef
    val paused: Boolean

    val buildCounter: Int
    val buildNumberFormat: String
}

interface BuildProblem {
    val id: BuildProblemId
    val type: BuildProblemType
    val identity: String
}

interface BuildProblemOccurrence {
    val buildProblem: BuildProblem
    val build: BuildRef
    val details: String
    val additionalData: String?
}

interface Parameter {
    val name: String
    val value: String
    val own: Boolean
}

interface Branch {
    val name: String?
    val isDefault: Boolean
}

interface BuildCommentInfo {
    val user: UserRef?
    val timestamp: ZonedDateTime
    val text: String
}

interface BuildAgentEnabledInfo {
    val user: UserRef?
    val timestamp: ZonedDateTime
    val text: String
}

interface BuildAgentAuthorizedInfo {
    val user: UserRef?
    val timestamp: ZonedDateTime
    val text: String
}

interface BuildCanceledInfo {
    val user: UserRef?
    val cancelDateTime: ZonedDateTime
    val text: String
}

interface Build : BuildRef {
    val buildConfigurationRef: BuildConfigurationRef
    val buildNumber: String?
    val status: BuildStatus?
    val branch: Branch
    val state: BuildState
    val personal: Boolean
    val name: String?
    val canceledInfo: BuildCanceledInfo?
    val comment: BuildCommentInfo?

    val composite: Boolean?

    val statusText: String?
    val queuedDateTime: ZonedDateTime
    val startDateTime: ZonedDateTime?
    val finishDateTime: ZonedDateTime?

    val runningInfo: BuildRunningInfo?

    val parameters: List<Parameter>

    val tags: List<String>

    /**
     * The same as revisions table on the build's Changes tab in TeamCity UI:
     * it lists the revisions of all of the VCS repositories associated with this build
     * that will be checked out by the build on the agent.
     */
    val revisions: List<Revision>

    /**
     * All snapshot-dependency-linked builds this build depends on
     */
    val snapshotDependencies: List<BuildRef>

    val pinInfo: PinInfo?

    val triggeredInfo: TriggeredInfo?

    val agentRef: BuildAgentRef?

    val detachedFromAgent: Boolean
}

interface Investigation {
    val id: InvestigationId
    val assignee: UserRef
    val reporter: UserRef?
    val comment: String
    val resolveMethod: InvestigationResolveMethod
    val targetType: InvestigationTargetType
    val testIds: List<TestId>?
    val problemIds: List<BuildProblemId>?
    val scope: InvestigationScope
    val state: InvestigationState
}

interface Mute {
    val id: InvestigationId
    val assignee: UserRef?
    val reporter: UserRef?
    val comment: String
    val resolveMethod: InvestigationResolveMethod
    val targetType: InvestigationTargetType
    val testIds: List<TestId>?
    val problemIds: List<BuildProblemId>?
    val scope: InvestigationScope
    val tests: List<TestRef>?
}

interface Test : TestRef {
    val name: String
}

interface BuildRunningInfo {
    val percentageComplete: Int
    val elapsedSeconds: Long
    val estimatedTotalSeconds: Long
    val outdated: Boolean
    val probablyHanging: Boolean
}

interface Change : ChangeRef {
    val version: String
    val username: String
    val user: UserRef?
    val dateTime: ZonedDateTime
    val comment: String
    val vcsRootInstance: VcsRootInstance?
}

interface ChangeFile {
    val fileRevisionBeforeChange: String?
    val fileRevisionAfterChange: String?
    val changeType: ChangeType

    /**
     * Full file path, may include VCS URL
     */
    val filePath: String?

    /**
     * File path relative to VCS root directory.
     */
    val relativeFilePath: String?
}

interface User : UserRef {
    val username: String
    val name: String?
    val email: String?
}

interface BuildArtifact {
    /** Artifact name without path. e.g. my.jar */
    val name: String
    /** Artifact name with path. e.g. directory/my.jar */
    val fullName: String
    val size: Long?
    val modificationDateTime: ZonedDateTime

    val build: BuildRef

    suspend fun download(output: File)
    suspend fun download(output: OutputStream)
    suspend fun openArtifactInputStream(): InputStream
}

interface VcsRoot : VcsRootRef {
    val name: String
    val url: String?
    val defaultBranch: String?
}

interface BuildAgent : BuildAgentRef {
    val name: String
    val pool: BuildAgentPoolRef
    val connected: Boolean
    val enabled: Boolean
    val authorized: Boolean
    val outdated: Boolean
    val ipAddress: String
    val parameters: List<Parameter>
    val enabledInfo: BuildAgentEnabledInfo?
    val authorizedInfo: BuildAgentAuthorizedInfo?
    val currentBuild: BuildRef?
}

interface BuildAgentPool : BuildAgentPoolRef {
    val name: String
    val projects: List<ProjectRef>
    val agents: List<BuildAgentRef>
}

interface VcsRootInstance {
    val vcsRootId: VcsRootId
    val name: String
}

interface PinInfo {
    val user: UserRef
    val dateTime: ZonedDateTime
}

interface Revision {
    val version: String
    val vcsBranchName: String
    val vcsRootInstance: VcsRootInstance
}

interface TestRun : TestRunRef {
    val name: String
    val status: TestStatus

    /**
     * Test run duration. It may be ZERO if a test finished too fast (<1ms)
     */
    val duration: Duration

    val details : String
    val ignored: Boolean

    /**
     * Current 'muted' status of this test on TeamCity
     */
    val currentlyMuted: Boolean

    /**
     * Muted at the moment of running tests
     */
    val mutedAtRunningTime: Boolean

    /**
     * Newly failed test or not
     */
    val newFailure: Boolean

    val build: BuildRef
    val fixedIn: BuildRef?
    val firstFailedIn: BuildRef?
    val test: TestRef
    val metadataValues: List<String>?
}

interface TriggeredInfo {
    val user: UserRef?
    val build: BuildRef?
}

interface FinishBuildTrigger {
    val initiatedBuildConfiguration: BuildConfigurationId
    val afterSuccessfulBuildOnly: Boolean
    val includedBranchPatterns: Set<String>
    val excludedBranchPatterns: Set<String>
}

interface ArtifactDependency {
    val dependsOnBuildConfiguration: BuildConfigurationRef
    val branch: String?
    val artifactRules: List<ArtifactRule>
    val cleanDestinationDirectory: Boolean
}

interface ArtifactRule {
    val include: Boolean
    /**
     * Specific file, directory, or wildcards to match multiple files can be used. Ant-like wildcards are supported.
     */
    val sourcePath: String
    /**
     * Follows general rules for sourcePath: ant-like wildcards are allowed.
     */
    val archivePath: String?
    /**
     * Destination directory where files are to be placed.
     */
    val destinationPath: String?
}

interface BuildQueue {
    suspend fun removeBuild(id: BuildId, comment: String = "", reAddIntoQueue: Boolean = false)
    fun queuedBuilds(projectId: ProjectId? = null): Flow<BuildRef>
    fun queuedBuilds(buildConfigurationId: BuildConfigurationId): Flow<BuildRef>
}

sealed class InvestigationScope {
    class InProject(val project: ProjectRef): InvestigationScope()
    class InBuildConfiguration(val configuration: BuildConfigurationRef): InvestigationScope()
}
