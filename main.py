from key import aleph_key
import requests
import json
from tabulate import tabulate


def api_req(req, params={}, results=[]):
    base = "https://data.occrp.org/"
    headers = {"Authorization": aleph_key, "Accept": "application/json"}
    params["limit"] = params.get("limit", "1000")
    params["offset"] = params.get("offset", 0)
    if req[:23] != "https://data.occrp.org/":
        url = base + req
    else:
        url = req
    r = requests.get(url, params=params, headers=headers)
    res = r.json()
    for result in res["results"]:
        results.append(result)

    if res["offset"] + len(res["results"]) < res["total"]:
        params["offset"] = params["offset"] + int(params["limit"])
        return api_req(req, params, results)

    else:
        return results


def search_term(term):
    """ Find entities matching string """

    docs_with_term = get_search_docs(term)
    meta = "%s: %s documents with this term" % (
            term, len(docs_with_term))

    req = "api/1/entities"
    par = {"q": term, "limit": 1000}
    r = api_req(req, par, [])
    meta += "; %s entities found:" % len(r)
    return {"search_meta": meta, "results": aggregate_results(term, r)}


def aggregate_results(name, results):

    out = {"Entity": [], "Name": [], "Source": [], "Documents": []}

    for res in results:
        docs = []
        out["Name"].append(res["name"])
        out["Entity"].append(res["id"])
        if "dataset" in res:
            source = "datsets/%s" % res["dataset"]
        elif "collection_id" in res:
            source = "collections/%s" % res["collection_id"]
            docs = get_entity_docs(res["id"])
        out["Documents"].append(len(docs) or "")
        out["Source"].append(source)

    return out


def get_entity_docs(entity_id):
    """ Get list of documents tagged with entity """
    docs = []
    req = "api/1/query"
    par = {"filter:entities.id": entity_id, "limit": 1000}
    r = api_req(req, par, [])
    for res in r:
        docs.append(res["id"])
    return docs


def get_search_docs(term):
    """ Gets documents tagged with the search term """
    req = "api/1/query"
    par = {"q": term, "limit": 1000}
    return(api_req(req, par, []))


def make_nice(search_meta, results):
    table = search_meta + "\n"
    table += tabulate(results, headers="keys", tablefmt="psql")
    return table.encode('utf8')

# TODO: enter search at cl
# r = search_term("Vladimir Putin")
r = search_term("Levan Vasadze")
with open('out', 'w') as f:
    f.write(make_nice(r["search_meta"], r["results"]))
