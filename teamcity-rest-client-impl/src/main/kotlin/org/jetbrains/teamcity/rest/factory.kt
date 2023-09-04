package org.jetbrains.teamcity.rest

import org.jetbrains.teamcity.rest.coroutines.TeamCityCoroutinesInstance
import org.jetbrains.teamcity.rest.coroutines.createGuestAuthCoroutinesInstance
import org.jetbrains.teamcity.rest.coroutines.createHttpAuthCoroutinesInstance
import org.jetbrains.teamcity.rest.coroutines.createTokenAuthCoroutinesInstance


/**
 * Factory object to create new object of [TeamCityInstance] interface
 *
 * @see TeamCityInstance
 */
object TeamCityInstanceFactory {
  /**
   * Creates guest authenticated accessor
   * @param serverUrl HTTP or HTTPS URL to TeamCity server
   *
   * Used via reflection for backward compatibility for deprecated methods
   */
  @JvmStatic
  fun guestAuth(serverUrl: String): TeamCityInstance
          = createGuestAuthInstance(serverUrl)

  @JvmStatic
  fun guestAuthCoroutines(serverUrl: String): TeamCityCoroutinesInstance
          = createGuestAuthCoroutinesInstance(serverUrl)

  /**
   * Creates username/password authenticated accessor
   *
   * @param serverUrl HTTP or HTTPS URL to TeamCity server
   * @param username username
   * @param password password
   *
   * Used via reflection for backward compatibility for deprecated methods
   */

  @JvmStatic
  fun httpAuth(serverUrl: String, username: String, password: String): TeamCityInstance
          = createHttpAuthInstance(serverUrl, username, password)

  @JvmStatic
  fun httpAuthCoroutines(serverUrl: String, username: String, password: String): TeamCityCoroutinesInstance
          = createHttpAuthCoroutinesInstance(serverUrl, username, password)

  /**
   * Creates token based connection.
   * TeamCity access token generated on My Settings & Tools | Access Tokens
   *
   * @param serverUrl HTTP or HTTPS URL to TeamCity server
   * @param token token
   *
   * see https://www.jetbrains.com/help/teamcity/rest-api.html#RESTAPI-RESTAuthentication
   */
  @JvmStatic
  fun tokenAuth(serverUrl: String, token: String): TeamCityInstance
          = createTokenAuthInstance(serverUrl, token)

  @JvmStatic
  fun tokenAuthCoroutines(serverUrl: String, token: String): TeamCityCoroutinesInstance
          = createTokenAuthCoroutinesInstance(serverUrl, token)
}
