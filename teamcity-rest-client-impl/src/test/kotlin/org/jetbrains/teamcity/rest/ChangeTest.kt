package org.jetbrains.teamcity.rest

import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

class ChangeTest {
    @Before
    fun setupLog4j() {
        setupLog4jDebug()
    }
    @Test
    fun test_equals_hashcode() {
        val id = publicInstance().builds().all().first { it.changes.isNotEmpty() }.changes.first().id

        val firstBlocking = publicInstance().change(id)
        val secondBlocking = publicInstance().change(id)
        assertEquals(firstBlocking, secondBlocking)

        runBlocking {
            val first = publicCoroutinesInstance().change(id)
            val second = publicCoroutinesInstance().change(id)
            assertEquals(first, second)
        }
    }

    @Test
    fun webUrl() {
        val configuration = publicInstance().buildConfiguration(changesBuildConfiguration)
        val change = publicInstance().builds()
                .fromConfiguration(configuration.id)
                .limitResults(10)
                .all()
                .firstOrNull { it.changes.isNotEmpty() }
                .let { build ->
                    assert(build != null) {
                        "Unable to find a build with changes (tried top 10) in ${configuration.getHomeUrl(branch = "<default>")}"
                    }

                    build!!.changes.first()
                }
        assertEquals(
                "$publicInstanceUrl/change/${change.id.stringId}",
                change.getHomeUrl()
        )
        assertEquals(
                "$publicInstanceUrl/change/${change.id.stringId}?buildTypeId=xxx&personal=true",
                change.getHomeUrl(specificBuildConfigurationId = BuildConfigurationId("xxx"), includePersonalBuilds = true)
        )
    }

    @Test
    fun changeByVcsRevision() {
        val build = publicInstance().builds()
                .fromConfiguration(changesBuildConfiguration)
                .limitResults(10)
                .all()
                .first { it.changes.isNotEmpty() }
        val change = build.changes.first()

        assertEquals(
                change.id,
                publicInstance().change(changesBuildConfiguration, change.version).id
        )
        assertTrue(change.firstBuilds().map { it.id }.contains(build.id))
    }

    @Test
    fun buildByVcsRevision() {
        val build = publicInstance().builds()
                .fromConfiguration(changesBuildConfiguration)
                .limitResults(10)
                .all()
                .first { it.changes.isNotEmpty() }
        val change = build.changes.first()

        val builds = publicInstance().builds()
                .fromConfiguration(changesBuildConfiguration)
                .withVcsRevision(change.version)
                .all()
        assertTrue(builds.map { it.id }.contains(build.id))
    }

    @Test
    fun associatedVcsRootReported() {
        val build = publicInstance().builds()
                .fromConfiguration(changesBuildConfiguration)
                .limitResults(10)
                .all()
                .firstOrNull { it.changes.isNotEmpty() && it.changes.any { change -> change.vcsRootInstance != null }}

        assertNotNull(build, "no vcsRootInstance found")
    }
}