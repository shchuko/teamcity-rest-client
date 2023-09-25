/**
 * Extended API required for `blocking -> coroutines` API bridges
 *
 * Why: `Flow<T>` cannot be easily converted to `Sequence<T>`,
 *       so we cannot reuse `fun all(): Flow<T>` by `runBlocking { }` wrapper
 *
 * Solution: All locators, in addition to `fun all(): Flow<T>`
 *           have `fun allSeq(): Sequence<T>` method to retrieve data blocking, but lazily
 */
package org.jetbrains.teamcity.rest.coroutines

import org.jetbrains.teamcity.rest.BuildConfigurationId
import org.jetbrains.teamcity.rest.ProjectId
import org.jetbrains.teamcity.rest.TeamCityInstanceBuilder
import org.jetbrains.teamcity.rest.TestStatus

internal interface TeamCityCoroutinesInstanceEx : TeamCityCoroutinesInstance {
    fun toBuilder(): TeamCityInstanceBuilder
}

internal interface BuildRefEx : BuildRef {
    fun getTestRunsSeq(status: TestStatus? = null): Sequence<TestRunRef>
    fun getBuildProblemsSeq(): Sequence<BuildProblemOccurrence>
}

internal interface VcsRootLocatorEx : VcsRootLocator {
    fun allSeq(): Sequence<VcsRootRef>
}

internal interface BuildAgentLocatorEx : BuildAgentLocator {
    fun allSeq(): Sequence<BuildAgentRef>
}

internal interface BuildAgentPoolLocatorEx : BuildAgentPoolLocator {
    fun allSeq(): Sequence<BuildAgentPoolRef>
}

internal interface BuildLocatorEx : BuildLocator {
    fun allSeq(): Sequence<BuildRef>
}

internal interface InvestigationLocatorEx : InvestigationLocator {
    fun allSeq(): Sequence<Investigation>
}


internal interface MuteLocatorEx : MuteLocator {
    fun allSeq(): Sequence<Mute>
}

internal interface TestLocatorEx : TestLocator {
    fun allSeq(): Sequence<TestRef>
}

internal interface TestRunsLocatorEx : TestRunsLocator {
    fun allSeq(): Sequence<TestRunRef>
}

internal interface UserLocatorEx : UserLocator {
    fun allSeq(): Sequence<UserRef>
}

internal interface BuildQueueEx : BuildQueue {
    fun queuedBuildsSeq(projectId: ProjectId? = null): Sequence<BuildRef>
    fun queuedBuildsSeq(buildConfigurationId: BuildConfigurationId): Sequence<BuildRef>
}
