import requests

# Configuration
KAPI_URL = "URL"
KAPI_USER = "KAPI_USER"
KAPI_PASSWORD = "KAPI_PASSWORD"
CA_CERT = "/etc/ssl/certs/ca-certificates.crt"


def get_access_token() -> str:
    """Authenticate and return access token."""
    url = f"{KAPI_URL}/v3/auth/token/"
    payload = {
        "grant_type": "",
        "username": KAPI_USER,
        "password": KAPI_PASSWORD,
        "scope": "",
        "client_id": "",
        "client_secret": ""
    }
    response = requests.post(url, data=payload, verify=CA_CERT)
    response.raise_for_status()
    return response.json()["access_token"]


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


def get_topics_size(headers: dict) -> dict:
    """Return dict of topic -> message count."""
    response = requests.get(f"{KAPI_URL}/v3/topics/size", headers=headers, verify=CA_CERT)
    response.raise_for_status()
    return response.json()


def get_consumer_groups(headers: dict) -> list:
    """Return consumer groups."""
    response = requests.get(f"{KAPI_URL}/v3/cgroups", headers=headers, verify=CA_CERT)
    response.raise_for_status()
    return response.json()


def analyze_topics(topics_size: dict, topic_consumers: dict) -> dict:
    """Analyze topics and categorize them."""
    all_topics = set(topics_size.keys())
    topics_with_consumers = set(topic_consumers.keys())

    # Topics vides (0 messages)
    empty_topics = {t for t, count in topics_size.items() if count == 0}

    # Topics sans consumers
    topics_without_consumers = all_topics - topics_with_consumers

    # Topics vides ET sans consumers
    empty_without_consumers = empty_topics & topics_without_consumers

    # Topics non vides sans consumers (ATTENTION: données non consommées!)
    non_empty_without_consumers = topics_without_consumers - empty_topics

    return {
        "empty": empty_topics,
        "without_consumers": topics_without_consumers,
        "empty_without_consumers": empty_without_consumers,
        "non_empty_without_consumers": non_empty_without_consumers
    }


def main():
    # Auth
    access_token = get_access_token()
    headers = {"Authorization": f"Bearer {access_token}"}

    # Get data
    topics_size = get_topics_size(headers)
    cgroups = get_consumer_groups(headers)
    topic_consumers = get_topic_consumers(cgroups)

    # Analyze
    analysis = analyze_topics(topics_size, topic_consumers)

    # Results
    print(f"Total topics: {len(topics_size)}")
    print(f"Topics vides: {len(analysis['empty'])}")
    print(f"Topics sans consumers: {len(analysis['without_consumers'])}")
    print(f"Topics vides ET sans consumers: {len(analysis['empty_without_consumers'])}")
    print(f"Topics NON vides sans consumers: {len(analysis['non_empty_without_consumers'])}")

    # Détail des topics non vides sans consumers (potentiel problème)
    if analysis["non_empty_without_consumers"]:
        print("\n⚠️  Topics avec données mais sans consumers:")
        for topic in sorted(analysis["non_empty_without_consumers"]):
            print(f"  - {topic}: {topics_size[topic]} messages")


if __name__ == "__main__":
    main()
