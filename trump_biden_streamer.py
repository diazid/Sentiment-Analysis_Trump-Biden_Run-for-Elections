import os
import json
from datetime import datetime
import requests
import time

## setting bearer token
bearer_token = os.environ.get("BEARER_TOKEN")

def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2FilteredStreamPython"
    return r

def get_rules():
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream/rules", auth=bearer_oauth
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot get rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))
    return response.json()

def delete_all_rules(rules):
    if rules is None or "data" not in rules:
        return None

    ids = list(map(lambda rule: rule["id"], rules["data"]))
    payload = {"delete": {"ids": ids}}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload
    )
    if response.status_code != 200:
        raise Exception(
            "Cannot delete rules (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    print(json.dumps(response.json()))

def set_rules(delete, query, tag):
    # You can adjust the rules if needed
    sample_rules = [
        {"value": query, "tag": tag}
    ]
    payload = {"add": sample_rules}
    response = requests.post(
        "https://api.twitter.com/2/tweets/search/stream/rules",
        auth=bearer_oauth,
        json=payload,
    )
    if response.status_code != 201:
        raise Exception(
            "Cannot add rules (HTTP {}): {}".format(response.status_code, response.text)
        )
    print(json.dumps(response.json()))

def get_stream(set, file_name):
    response = requests.get(
        "https://api.twitter.com/2/tweets/search/stream?tweet.fields=author_id,context_annotations,lang,created_at,public_metrics,source&expansions=attachments.media_keys&media.fields=public_metrics,duration_ms&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type", auth=bearer_oauth, stream=True,
    )
    print(response.status_code)
    if response.status_code != 200:
        raise Exception(
            "Cannot get stream (HTTP {}): {}".format(
                response.status_code, response.text
            )
        )
    for response_line in response.iter_lines():
        if response_line:
            json_response = json.loads(response_line)
            print(json.dumps(json_response, indent=4, sort_keys=True))

            #saving into json file
            with open(file_name, 'a') as f:
                json.dump(json_response, f)
                f.write('\n')

def now():
    now = datetime.now()
    date_string = now.strftime("%Y%m%d-%H%M%S")
    folder = 'data'
    filename = f'{folder}/tweets_{date_string}.json'
    return filename

def main():
    rules = get_rules()
    delete = delete_all_rules(rules)
    query = 'entity:"Donald Trump" OR entity:"Joe Biden" lang:en place_country:US has:geo -is:nullcast '
    tag = "politics"
    set = set_rules(delete, query, tag)
    filename = now()  # get the filename
    get_stream(set, file_name=filename)

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(e)
            time.sleep(5*60)