# mvn profiles for the different supported 
# Spark and Scala versions. Uncomment
# the one that you want to use. You can also 
# override the profile on the command line:
# `make MVN_PROFILE=spark-2.4-scala-2.11 build`
MVN_PROFILE := spark-3.0-scala-2.12
# MVN_PROFILE := spark-2.4-scala-2.11
# MVN_PROFILE := spark-2.3-scala-2.11
# MVN_PROFILE := spark-2.2-scala-2.11

# Build the project for specific Spark and 
# Scala versions. You can change the profile 
# variable to use a different Scala or Spark 
# version (see list above).
# If you need more log ouput remove the -q flag.
build:
	mvn clean install -q -P $(MVN_PROFILE)

# Deprecated.
travis-deploy:
	gpg --import .travis/private-signing-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -P release --settings .travis/settings.xml
	mvn clean deploy -P release -P $(MVN_PROFILE) --settings .travis/settings.xml
