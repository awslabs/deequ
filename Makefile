# mvn profiles for the different supported 
# Spark and Scala versions
MVN_PROFILE_SPARK_30 := spark-3.0-scala-2.12
MVN_PROFILE_SPARK_24 := spark-2.4-scala-2.11
MVN_PROFILE_SPARK_23 := spark-2.3-scala-2.11
MVN_PROFILE_SPARK_22 := spark-2.2-scala-2.11

# Build the project for specific Spark and 
# Scala versions. You can change the profile 
# variable to use a differen Scala or Spark 
# version (see list above).
build:
	mvn clean install -P $(MVN_PROFILE_SPARK_30)

# Test if there are build issues for any Scala 
# or Spark version.
build-and-test-all-profiles:
	mvn clean install -q -P $(MVN_PROFILE_SPARK_30)
	mvn clean install -q -P $(MVN_PROFILE_SPARK_24)
	mvn clean install -q -P $(MVN_PROFILE_SPARK_23)
	mvn clean install -q -P $(MVN_PROFILE_SPARK_22)

# Deprecated.
travis-deploy:
	gpg --import .travis/private-signing-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -P release --settings .travis/settings.xml
	mvn clean deploy -P release -P $(MVN_PROFILE_SPARK_30) --settings .travis/settings.xml
