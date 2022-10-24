build:
	mvn clean install
compile:
	mvn clean compile
single-test:
	mvn clean test -Dtest="TheFirstUnitTest"