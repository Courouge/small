import requests
def get_topic_consumers(cgroups: list) -> dict:
    """Build dict of topic -> list of consumers."""
    topic_consumers = {}
    for group in cgroups:
        for member in group.get("members", []):
            for sub_topic in member.get("subscription", []):
                if sub_topic not in topic_consumers:
                    topic_consumers[sub_topic] = []
                topic_consumers[sub_topic].append({
                    "group": group["group"],
                    "state": group["state"],
                    "client_id": member["client_id"],
                    "client_host": member["client_host"]
                })
    return topic_consumers

KAPI_URL="URL"

KAPI_USER="KAPI_USER"
KAPI_PASSWORD="KAPI_PASSWORD"

url = f"{KAPI_URL}/v3/auth/token/"

playload = {
    "grant_type": "",
    "username": KAPI_USER,
    "password": KAPI_PASSWORD,
    "scope": "",
    "client_id": "",
    "client_secret": ""
}
# Envoi de la requête POST
response = requests.post(url, data=playload, verify="/etc/ssl/certs/ca-certificates.crt")
data = response.json()
access_token = data["access_token"]

headers = {"Authorization": f"Bearer {access_token}"}
# Get topics size
response = requests.get(f"{KAPI_URL}/v3/topics/size", headers=headers, verify="/etc/ssl/certs/ca-certificates.crt")
topics = response.json()

empty_topics = [topic for topic, count in topics.items() if count == 0]
nb_empty_topics = len(empty_topics)
print(nb_empty_topics)

# Get consumer groups
response = requests.get(f"{KAPI_URL}/v3/cgroups", headers=headers, verify="/etc/ssl/certs/ca-certificates.crt")
cgroups = response.json()
topic_consumers = get_topic_consumers(cgroups)
print(topic_consumers)
