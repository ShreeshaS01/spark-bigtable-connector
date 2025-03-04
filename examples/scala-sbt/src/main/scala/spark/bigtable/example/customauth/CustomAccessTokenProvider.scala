package spark.bigtable.example.customauth

import com.google.auth.oauth2.GoogleCredentials
import com.google.cloud.spark.bigtable.{AccessToken, AccessTokenProvider}

import java.io.IOException
import java.time.Instant
import java.util.Date

class CustomAccessTokenProvider extends AccessTokenProvider {
  // Initialize GoogleCredentials
  private val credentials: GoogleCredentials = GoogleCredentials.getApplicationDefault

  // Fetch the initial token and expiry time
  private var currentToken: String = fetchInitialToken()
  private var tokenExpiry: Instant = fetchInitialTokenExpiry()

  @throws(classOf[IOException])
  override def getAccessToken(): AccessToken = {
    if (isTokenExpired) {
      refresh()
    }
    new AccessToken(currentToken, Date.from(tokenExpiry))
  }

  @throws(classOf[IOException])
  override def refresh(): Unit = {
    println("Refreshing token...")
    // Refresh the credentials to get a new access token
    credentials.refresh()
    val newToken = credentials.getAccessToken.getTokenValue
    val newExpiry = credentials.getAccessToken.getExpirationTime.toInstant

    // Update the current token and expiry
    currentToken = newToken
    tokenExpiry = newExpiry
  }

  @throws(classOf[IOException])
  def getCurrentToken: String = currentToken

  private def isTokenExpired: Boolean = {
    Instant.now().isAfter(tokenExpiry)
  }

  @throws(classOf[IOException])
  private def fetchInitialToken(): String = {
    // Ensure the credentials are refreshed before fetching the token
    credentials.refreshIfExpired()
    credentials.getAccessToken.getTokenValue
  }

  @throws(classOf[IOException])
  private def fetchInitialTokenExpiry(): Instant = {
    credentials.refreshIfExpired()
    credentials.getAccessToken.getExpirationTime.toInstant
  }
}