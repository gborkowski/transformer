#!/bin/sh

export BASE=/Users/glen.borkowski/BR/java/vscode_projects/transformer/target

export CLASSPATH=$CLASSPATH:$BASE/../log4j2.xml
export CLASSPATH=$CLASSPATH:$BASE/transformer-1.0-SNAPSHOT-jar-with-dependencies.jar

# There is one required argument for each phase:
# specify the sub directory you want to use
# best practices say to use customer name:
#
# ../properties/Graybar/importPhase.properties
#
# for the above, you would use Graybar as the argument

#java com.br.transform.ImportPhase Petco
java com.br.transform.TransformPhase VS
#java com.br.transform.OutputPhase VS
