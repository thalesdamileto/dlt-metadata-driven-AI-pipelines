import json
import requests

DATABRICKS_HOST = "https://<seu-workspace>.azuredatabricks.net"
TOKEN = "<seu-token>"

HEADERS = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

CONFIG_FILE = "config/1-bronze/databricks_volume/json/dlt_volume_to_json.json"


def get_existing_pipelines():
    url = f"{DATABRICKS_HOST}/api/2.0/pipelines"
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    pipelines = response.json().get("statuses", [])

    return {p["name"]: p["pipeline_id"] for p in pipelines}


def create_pipeline(config):
    url = f"{DATABRICKS_HOST}/api/2.0/pipelines"
    r = requests.post(url, headers=HEADERS, json=config)
    r.raise_for_status()
    print(f"Pipeline criado: {config['name']}")


def update_pipeline(pipeline_id, config):
    url = f"{DATABRICKS_HOST}/api/2.0/pipelines/{pipeline_id}"
    r = requests.put(url, headers=HEADERS, json=config)
    r.raise_for_status()
    print(f"Pipeline atualizado: {config['name']}")


# -----------------------------
# execução principal
# -----------------------------

with open(CONFIG_FILE) as f:
    pipeline_configs = json.load(f)

existing = get_existing_pipelines()

for config in pipeline_configs:

    name = config["name"]

    if name in existing:
        update_pipeline(existing[name], config)
    else:
        create_pipeline(config)
