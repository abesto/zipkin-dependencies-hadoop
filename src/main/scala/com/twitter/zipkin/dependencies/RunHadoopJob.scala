package com.twitter.zipkin.dependencies

object RunHadoopJob extends App {
  com.twitter.scalding.Tool.main(Array("com.twitter.zipkin.dependencies.ZipkinDependenciesJob","--hdfs") ++ args)
}
