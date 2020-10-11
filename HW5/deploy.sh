#!/bin/bash
sbt assembly
mv target/scala-2.11/hw5-assembly-0.1.jar target/scala-2.11/hw5.jar
scp target/scala-2.11/hw5.jar a.karablinov@rf-cluster.dadadata.ru:hw5
scp script.py a.karablinov@rf-cluster.dadadata.ru:hw5