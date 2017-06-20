import sys
import requests
import json
import csv
from key import aleph_key


def get_search_terms(csv_fn, columns, delimiter=',', quotechar='"'):
    """ 
    Takes a csv file and a list of the names of the columns to use 
    for crossreferencing 
    """
    try:
        with open(csv_fn, 'rb') as csvfile:
            csvreader = csv.reader(
                csvfile, delimiter=delimiter, quotechar=quotechar)
            # Assume the first row has the column names.
            colnames = next(csvreader)
            usecols = []
            search_terms = []
            for column in columns:
                try:
                    usecols.append(colnames.index(column))
                except ValueError:
                    print "Column '%s' was not found in the input." % column
            for row in csvreader:
                for i in usecols:
                    search_terms.append(row[i])
        search_terms = filter(None, set(search_terms))
        print "%s unique search terms found.." % len(search_terms)
        return search_terms
    except IOError:
        print "File not found. More information: https://http.cat/404"
        return []


def api_req(req, params=None, results=None):
    if results is None:
        results = []
    if params is None:
        params = {}

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
    if res.get("results") is not None:
        for result in res["results"]:
            results.append(result)
        if res["offset"] + len(res["results"]) < res["total"]:
            params["offset"] = params["offset"] + int(params["limit"])
            return api_req(req, params, results)
    else:
        results.append(res)

    return results


def search_term(term):
    """ Find entities matching string """

    term = '"%s"' % term
    docs_with_term = get_search_docs(term)

    req = "api/1/entities"
    par = {"q": term, "limit": 1000}
    r = api_req(req, par, [])

    print "Found %s documents and %s entities." % (len(docs_with_term), len(r))

    return {"input": term, "docs": len(docs_with_term), "entities": aggregate_results(r)}


def aggregate_results(results):
    if len(results) > 0:
        out = []

        for res in results:
            docs = []
            if "dataset" in res:
                source = "https://data.occrp.org/entities?filter:dataset=%s" % res[
                    "dataset"]
                source_name = res["dataset"]
            elif "collection_id" in res:
                source = "https://data.occrp.org/collections/%s" % res["collection_id"]
                source_name = None
                docs = get_entity_docs(res["id"])

            out.append({"name": res["name"], "id": res[
                       "id"], "source": source, "source_name": source_name, "docs": len(docs), "meta": res})

        return out
    return {}


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


def label_collection(collection_id):
    req = "api/1/collections/%s" % collection_id
    par = {"limit": 1000}
    r = api_req(req, par, [])[0]
    return r.get("label", "Untitled")


def html_start():
    return """<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>alpeh xref</title>
    <link type="text/css" href="https://data.occrp.org/static/assets/aleph.css" rel="stylesheet" />
  </head>
  <body id="page"><div class="help screen">
    <h1>Crossreferencing results</h1>
    <table class="table">
      <thead><tr>
        <th>Input</th>
        <th>Documents</th>
        <th colspan="5">Entities</th>
      </tr></thead>
      <tbody>"""


def html_end():
    return """
      </tbody>
    </table>
  </div></body>
</html>"""


def html_results(results):
    html = """
      <tr>
        <td><strong>%s</strong></td>
        <td><a href="https://data.occrp.org/documents?q=%s">%s</a></td>
        <td colspan="5"><a href="https://data.occrp.org/entities?q=%s">%s</a></td>
      </tr>
    """ % (results["input"], results["input"], results["docs"], results["input"], len(results["entities"]))
    if len(results["entities"]) > 0:
        html += """
      <tr>
        <th></th>
        <th></th>
        <th>Entity</th>
        <th>Source</th>
        <th>Documents</th>
        <th>Countries</th>
        <th>Dates</th>
      </tr>
        """
        for entity in results['entities']:
            html += """
      <tr>
        <td></td>
        <td></td>
        <td><a href="https://data.occrp.org/entities/%s">%s</a></td>
        <td><a href="%s">%s</a></td>
        <td>""" % (entity["id"], entity["name"], entity["source"], entity["source_name"])

            if entity["docs"] > 0:
                html += """<a href="https://data.occrp.org/documents?filter:entities.id=%s">%s</a>""" % (
                    entity["id"], entity["docs"])

            html += """
        </td><td>"""
            if len(entity["meta"].get("countries", [])) > 0:
                html += ", ".join(entity["meta"]["countries"])
            html += """
        </td><td>"""
            if len(entity["meta"].get("dates", [])) > 0:
                html += ", ".join(entity["meta"]["dates"])
            html += """
        </td><td>"""
            html += """
      </tr>"""
        html += """
    </td></tr>"""

    return html.encode('utf8')


def run(filename, *column_names):
    terms = get_search_terms(filename, column_names[0])
    r = []
    for i, term in enumerate(terms):
        print "Searching (%s/%s) ... %s ..." % (i, len(terms), term)
        r.append(search_term(term))

    # Sort results by entities then documents found
    r = sorted(r, key=lambda r: (len(r["entities"]), r["docs"]), reverse=True)

    sources = {}

    with open('out.html', 'w') as f:
        f.write(html_start())
        for res in r:
            
            for entity in res["entities"]:
                if entity["source_name"] is None:
                    if sources.get(entity["source"], None) is None:
                        sources[entity["source"]] = label_collection(entity["meta"]["collection_id"])
                    entity["source_name"] = sources[entity["source"]]

            f.write(html_results(res))
        f.write(html_end())

    print "Result output to file: `out.html`"

if __name__ == "__main__":
    run(sys.argv[1], sys.argv[2:])
