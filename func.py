import io
import json
import gzip
import os
import base64
import requests
import sys

SPLUNK_HEC_URL = os.environ.get("SPLUNK_HEC_URL")
SPLUNK_HEC_TOKEN = os.environ.get("SPLUNK_HEC_TOKEN")


def normalize_payload(data):
    # BytesIO → bytes
    if isinstance(data, io.BytesIO):
        data = data.read()

    # gzip → bytes
    if isinstance(data, (bytes, bytearray)) and data.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            data = f.read()

    # bytes → str
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")

    # str → JSON
    if isinstance(data, str):
        data = json.loads(data)

    # Case 1: wrapper with "records"
    if isinstance(data, dict) and "records" in data:
        events = []
        for record in data["records"]:
            decoded = base64.b64decode(record["value"]).decode("utf-8")
            events.append(json.loads(decoded))
        return events

    # Case 2: list of streaming records
    if isinstance(data, list):
        events = []
        for record in data:
            if isinstance(record, dict) and "value" in record:
                decoded = base64.b64decode(record["value"]).decode("utf-8")
                events.append(json.loads(decoded))
            else:
                events.append(record)
        return events

    # Case 3: single streaming record
    if isinstance(data, dict) and "value" in data:
        decoded = base64.b64decode(data["value"]).decode("utf-8")
        return json.loads(decoded)

    return data


def handler(ctx, data=None):
    try:
        if data is None:
            return {"status": "no data"}

        payload = normalize_payload(data)

        # Ensure list
        events = payload if isinstance(payload, list) else [payload]

        headers = {
            "Authorization": f"Splunk {SPLUNK_HEC_TOKEN}",
            "Content-Type": "application/json"
        }

        for event in events:
            splunk_event = {
                "source": "oci:datasafe",
                "sourcetype": "_json",
                "event": event
            }

            response = requests.post(
                SPLUNK_HEC_URL,
                headers=headers,
                data=json.dumps(splunk_event),
                timeout=10,
                verify=False   # Required for Splunk Cloud SSL chain
            )

            if response.status_code != 200:
                raise Exception(
                    f"Splunk HEC error {response.status_code}: {response.text}"
                )

        sys.stdout.write(f"Sent {len(events)} event(s) to Splunk\n")
        sys.stdout.flush()

        return {
            "status": "success",
            "events_sent": len(events)
        }

    except Exception as e:
        sys.stdout.write(f"Function error: {str(e)}\n")
        sys.stdout.flush()
        raise

