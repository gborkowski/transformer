#!/bin/sh

export BASE=/Users/glen.borkowski/BR/java/vscode_projects/transformer/target
export GOOGLE_APPLICATION_CREDENTIALS=$BASE/../transform-2a333-firebase-adminsdk-26yti-b42ad15736.json

export CLASSPATH=$CLASSPATH:$BASE/../log4j2.xml
export CLASSPATH=$CLASSPATH:$BASE/transformer-1.0-SNAPSHOT-jar-with-dependencies.jar

# There is one required argument for each phase:
# specify the customer you want to use

java com.br.transform.ImportPhase Jumbo
#java com.br.transform.TransformPhase Jumbo
#java com.br.transform.OutputPhase Jumbo
