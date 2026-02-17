import io
import json
import gzip
import os
import base64
import sys
import requests


# Splunk HEC configuration (provided via OCI Function environment variables)
SPLUNK_HEC_URL = os.getenv("SPLUNK_HEC_URL")
SPLUNK_HEC_TOKEN = os.getenv("SPLUNK_HEC_TOKEN")


def normalize_payload(data):

    # Convert BytesIO payloads to raw bytes
    if isinstance(data, io.BytesIO):
        data = data.read()

    # Decompress gzip payloads
    if isinstance(data, (bytes, bytearray)) and data.startswith(b"\x1f\x8b"):
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as f:
            data = f.read()

    # Decode bytes to string
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8")

    # Parse JSON string payloads
    if isinstance(data, str):
        data = json.loads(data)

    events = []

    # Payload wrapped in a "records" array (common with OCI Streaming)
    if isinstance(data, dict) and "records" in data:
        for record in data["records"]:
            decoded = base64.b64decode(record["value"]).decode("utf-8")
            events.append(json.loads(decoded))
        return events

    # List of streaming records
    if isinstance(data, list):
        for record in data:
            if isinstance(record, dict) and "value" in record:
                decoded = base64.b64decode(record["value"]).decode("utf-8")
                events.append(json.loads(decoded))
            else:
                events.append(record)
        return events

    # Single streaming record
    if isinstance(data, dict) and "value" in data:
        decoded = base64.b64decode(data["value"]).decode("utf-8")
        return [json.loads(decoded)]

    # Default case: treat the payload as a single event
    return [data]


def handler(ctx, data=None):

    if not data:
        sys.stdout.write("No payload received\n")
        sys.stdout.flush()
        return {"status": "no_data"}

    events = normalize_payload(data)

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
            verify=False  # Required for some Splunk Cloud SSL chains
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

