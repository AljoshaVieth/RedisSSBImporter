ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.2.2"

lazy val root = (project in file("."))
  .settings(
    name := "RedisSSBInserter",
    idePackagePrefix := Some("de.aljoshavieth.redisssbinserter")
  )
// https://mvnrepository.com/artifact/redis.clients/jedis
libraryDependencies += "redis.clients" % "jedis" % "4.4.0-m2"
libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.5"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.5"



