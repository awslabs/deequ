MVN_PROFILE := spark-3.0-scala-2.12

build:
	mvn install
	mvn clean install -P $(MVN_PROFILE)

travis-deploy:
	gpg --import .travis/private-signing-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -P release --settings .travis/settings.xml
	mvn clean deploy -P release -P $(MVN_PROFILE) --settings .travis/settings.xml
