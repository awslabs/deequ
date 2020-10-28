build:
	mvn install
	mvn clean install -P scala-2.12

travis-deploy:
	gpg --import .travis/private-signing-key.gpg
	mvn versions:set -DnewVersion=${TRAVIS_TAG}
	mvn clean deploy -P release --settings .travis/settings.xml
	mvn clean deploy -P release -P scala-2.12 --settings .travis/settings.xml