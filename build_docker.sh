#!/bin/bash
CURRENTDATE=$(date +%Y%m%d%H%M)
VER="0.0.$CURRENTDATE"

dockerRegistry="registry-qa.webex.com"

rm -fr .git

IMAGEVER="$dockerRegistry/dba/dba-ansible-dbpatch:$VER"
docker build -t $IMAGEVER . --build-arg base_image=registry-qa.webex.com/rhel8/osaas-fips:1.10.202208
echo "built image success: $IMAGEVER"

docker push $IMAGEVER
echo "pushed image success: $IMAGEVER"
