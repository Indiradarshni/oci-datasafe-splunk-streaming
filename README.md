
## Serverless integration to stream Oracle Data Safe alerts to Splunk using OCI Streaming and OCI Functions

This repository contains a sample OCI Function written in Python that streams
Oracle Data Safe alerts from OCI Streaming to Splunk SIEM using HTTP Event Collector (HEC).

## Architecture

Oracle Data Safe  
→ OCI Events  
→ OCI Streaming  
→ Service Connector Hub  
→ OCI Functions  
→ Splunk HEC

## Contents

- func.py – OCI Function code to decode streaming records and send events to Splunk
- requirements.txt – Python dependencies

## Configuration

The function expects the following environment variables:

- SPLUNK_HEC_URL
- SPLUNK_HEC_TOKEN
