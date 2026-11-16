import argparse, json, time
from datetime import datetime, timezone
import requests

p = argparse.ArgumentParser()
p.add_argument("--config", default="/opt/iss-producer/config.json")
p.add_argument("--minutes", type=float, default=0)  # 0 = run until stopped
p.add_argument("--interval", type=float, default=10)  # seconds between API calls
p.add_argument("--dry-run", action="store_true")  # print, do not send
a = p.parse_args()

cfg = json.load(open(a.config))
URL = "http://api.open-notify.org/iss-now.json"

producer = None
if not a.dry_run:
    from azure.eventhub import EventHubProducerClient, EventData
    from azure.identity import ManagedIdentityCredential

    producer = EventHubProducerClient(
        fully_qualified_namespace=cfg["namespace"],
        eventhub_name=cfg["hub"],
        credential=ManagedIdentityCredential(),
    )

end = time.time() + a.minutes * 60 if a.minutes > 0 else None
sent = 0
try:
    while end is None or time.time() < end:
        t0 = time.time()
        try:
            r = requests.get(URL, timeout=10)
            r.raise_for_status()
            if a.dry_run:
                print(r.text.strip(), flush=True)
            else:
                batch = producer.create_batch()
                batch.add(EventData(r.text))  # raw API JSON is the event body
                producer.send_batch(batch)
            sent += 1
            print(datetime.now(timezone.utc).isoformat(), "sent", sent, flush=True)
        except Exception as e:
            print("ERROR", e, flush=True)
        time.sleep(max(0, a.interval - (time.time() - t0)))
finally:
    if producer:
        producer.close()
    print("STOPPED after", sent, "events", flush=True)
