#!/bin/bash
for DIR in {108年傷亡道路交通事故資料,109年傷亡道路交通事故資料,110年傷亡道路交通事故資料,111年傷亡道路交通事故資料}
do 
    for File in $DIR/*.csv
    do
        if [[ $File == *"交通事故資料"* ]]; then
            python data_preprocess.py $File
        fi
    done
done