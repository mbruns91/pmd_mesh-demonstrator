from types import SimpleNamespace
import ast
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context
from SPARQLWrapper import SPARQLWrapper
import requests
import re

def canonify_string(name):
    """
    Remove all invalid characters for python identifiers (everything not a letter, number or underscore) 
    """
    return re.sub(r'[^a-zA-Z0-9_]', '_', name)

class RecursiveNamespace(SimpleNamespace):
    @staticmethod
    def map_entry(entry):
        if isinstance(entry, dict):
            return RecursiveNamespace(**{
                canonify_string(k): v for k, v in entry.items()
            })
        return entry

    def __init__(self, **kwargs):
        new_kwargs = {canonify_string(key): val for key, val in kwargs.items()}
        super().__init__(**new_kwargs)
        for key, val in new_kwargs.items():
            if isinstance(val, dict):
                setattr(self, key, RecursiveNamespace(**val))
            elif isinstance(val, list):
                setattr(self, key, [self.map_entry(v) for v in val])

def recursive_iter(obj):
    for key, value in vars(obj).items():
        yield key, value  # recent level
        # Recursion for nested objects
        if isinstance(value, RecursiveNamespace):
            yield from recursive_iter(value)
        # handle lists which already contain RecursiveNamespace objects
        elif isinstance(value, list):
            for item in value:
                if isinstance(item, RecursiveNamespace):
                    yield from recursive_iter(item)

def rectify_endpoints(endpoint):
    ep_1 = endpoint.replace(":None/api/jena", "/api/v1/jena")
    ep_2 = ep_1.replace(":443/api/jena", "/api/v1/jena")
    return ast.literal_eval(ep_2)

def make_dataframe(result, columns):
    liste = []
    for r in result['results']['bindings']:
        row = []
        for k in r.keys():
            row.append(r[k]['value'])
        liste.append(row)
    df = pd.DataFrame(liste, columns = columns)
    return df

def send_query(endpoint: str | None = None,
               token: str | None = None,
               query: str | None = None,
               columns: list[str] | None = None,
               print_to_screen: bool = True
              ) -> pd.DataFrame:
    """
    Send a SPARQL query to a provided endpoint.
    Convert the result to a pandas dataframe and apply provided column headers.
    Optionally print some context to screen.
    Return the dataframe.

    Args:

    Returns:
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat('json')
    sparql.addCustomHttpHeader("Authorization", f'Bearer {token}')
    sparql.setQuery(query)
    result = sparql.queryAndConvert()
    result_df = make_dataframe(result, columns)
    if print_to_screen:
        print(f'Sending query to "{endpoint}". Result:')
        print(result_df)
        print("")
    return result_df

def query_instance(name: str,
                   address: str,
                   token: str,
                   datasets: list[str] = [""],
                   query: str | None = None,
                   columns: list[str] | None = None,
                   print_to_screen: bool = True
                  ):
    headers = {"Authorization": f"Bearer {token}"}
    endpoints = rectify_endpoints(requests.get(f'{address}/api/v1/endpoints', headers=headers).content.decode())
    
    results = {}
    name = canonify_string(name)
    if name not in results:
        results[name] = {}
    for ep in endpoints:
        if datasets is None or datasets == [""] or any(substring in ep for substring in datasets): # only query endpoints whose address contains any of the dataset names we passed
            dataset_name = ep.split("/")[-2] # is this robust enough?
            #print(dataset_name)
            if dataset_name not in results[name]:
                results[name][dataset_name] = {}
            data = send_query(endpoint=ep, token=token, query=query, columns=columns, print_to_screen=print_to_screen)
            results[name][dataset_name]["endpoint"] = ep
            results[name][dataset_name]["query"] = query
            results[name][dataset_name]["result"] = data

    return results

def federated_query(partners, datasets, query, columns, print_to_screen):
    for key in partners.__dict__:
        results = {}
        if key not in results:
            results[key] = {}
        name = getattr(partners, key).ontodocker.name
        address = getattr(partners, key).ontodocker.address
        token = getattr(partners, key).ontodocker.token
        results[key] = query_instance(name, address, token, datasets, query, columns, print_to_screen)
    return results