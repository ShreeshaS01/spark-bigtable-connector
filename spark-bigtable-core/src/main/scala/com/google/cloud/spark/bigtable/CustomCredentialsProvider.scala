package com.google.cloud.spark.bigtable

import com.google.api.gax.core.CredentialsProvider
import com.google.auth.Credentials

class CustomCredentialsProvider(private val credentials: Credentials) extends CredentialsProvider {

  override def getCredentials: Credentials = credentials

}
