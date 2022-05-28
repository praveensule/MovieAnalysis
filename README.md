# Spark Movie Analysis task

To execute the program correctly please make sure you have winutils configured in system as hadoop home

To test the application I have included input file and output files in the root directory. 

You can point any path by configuring the reference config.Below is the path for the same.
_MovieAnalysis\src\main\resources\reference.conf_

#Commands to execute the application:
* Download/ Clone the git repo
* Run gradlew build to download the required jars and create an artifact
* Run gradlew test to run the unit & integration test cases.

output-reference contains the output of the execution. 

you can configure the spark master from reference config file using the property **sparkClusterMode**