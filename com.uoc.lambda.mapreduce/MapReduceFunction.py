#
#
# main() will be run when you invoke this action
#
# @param Cloud Functions actions accept a single parameter, which must be a JSON object.
#
# @return The output of this action, which must be a JSON object.
#
#
import sys



#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import os
import ibm_boto3 as boto3
import json
from botocore.client import Config


def create_text_file(bucket_name, input_file, output_file):
    client = boto3.client('s3', ibm_api_key_id='****',
    ibm_service_instance_id='***'
    ,ibm_auth_endpoint='https://iam.bluemix.net/oidc/token',
    config=Config(signature_version='oauth'),
    endpoint_url='https://s3.us-south.cloud-object-storage.appdomain.cloud')
    s3Object = client.get_object(Bucket=bucket_name, Key=input_file)
    lines = s3Object['Body'].read().splitlines()
    word_map = {}
    for line in lines:
        words = line.split()
        for word in words:
            decoded_word = word.decode('utf-8')
            count = word_map.get(decoded_word, 0)
            word_map[decoded_word] = count + 1
    client.put_object(Bucket=bucket_name, Key=output_file,
                      Body=json.dumps(word_map),
                      Metadata={'Content-Type': 'plain/text'})


def main(dict):

    create_text_file(dict["buket"], dict["input"], dict["output"])
    return {"Status":'OK'}
