from key import aleph_key
import requests
import json
from tabulate import tabulate


def api_req(req, params={}):
    base = "https://data.occrp.org/"
    headers = {"Authorization": aleph_key, "Accept": "application/json"}
    url = base + req
    r = requests.get(url, params=params, headers=headers)
    return r


def search_name(name):
    """ Find entities matching name """
    out = {"Input": [name], "Entity": [],
           "Name": [], "Source": [], "Documents": []}
    req = "api/1/entities"
    par = {"q": name}
    r = api_req(req, par)
    for res in r.json()["results"]:
        out["Input"].append("")
        out["Name"].append(res["name"])
        out["Entity"].append(res["api_url"])

        if "dataset" in res:
            source = "https://data.occrp.org/api/1/datasets/" + res["dataset"]
        elif "collection_id" in res:
            source = "https://data.occrp.org/api/1/collections/" + \
                str(res["collection_id"])
            docs = get_entity_docs(res["id"])

        out["Source"].append(source)
        # TODO: Decide what to do with documents. Is a list of them useful? Maybe a count?
        # out["Documents"].append(docs)

    return out


def get_entity_docs(entity_id):
    """ Get list of documents tagged with entity """
    docs = []
    req = "api/1/query"
    par = {"entity": entity_id}
    r = api_req(req, par)
    for res in r.json()["results"]:
        docs.append("https://data.occrp.org/documents/%s" % res["id"])
    return docs


def make_nice(results):
    return tabulate(results, headers="keys", tablefmt="psql")

# TODO: enter search at cl
r = search_name("Levan Vasadze")
print make_nice(r)
