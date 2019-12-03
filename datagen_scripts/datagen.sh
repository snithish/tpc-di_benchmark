#!/usr/bin/env sh

# Usage: ./datagen.sh $SCALE

SCALE=$1

gsutil -m cp -r gs://tpcdi-datagen ~/tpcdi-datagen
cd ~/tpcdi-datagen || exit
rm -rf ~/data && mkdir -p ~/data
java -jar DIGen.jar -sf "$SCALE" -o ~/data
