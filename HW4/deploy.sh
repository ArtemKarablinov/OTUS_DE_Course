#!/bin/bash
sbt package
mv target/scala-2.11/HW4_2.11-0.1.jar target/scala-2.11/HW4.jar
scp target/scala-2.11/HW4.jar a.karablinov@rf-cluster.dadadata.ru:HW4