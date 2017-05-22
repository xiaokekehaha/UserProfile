#!/bin/bash
sbt assembly
scp /Users/liziyao/Git_bag/UserProfile/target/scala*/*.jar app:/data/lzy/lib
