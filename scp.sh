#!/bin/bash
sbt assembly
#scp /Users/liziyao/Git_bag/UserProfile/target/scala*/*.jar app:/data/lzy/lib
scp /Users/liziyao/Git_bag/UserProfile/target/scala*/*.jar recom_test:~/
