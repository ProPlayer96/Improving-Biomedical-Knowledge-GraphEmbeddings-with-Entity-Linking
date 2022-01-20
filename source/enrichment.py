import neo4j
import json
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import torch


class Neo4jConnector(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def clean_database(self):
        with self._driver.session() as session:
            result = session.write_transaction(self._delete_graph)
        return result

    def top_k_neighbors(self, region, k):
        with self._driver.session() as session:
            result = session.read_transaction(self._top_k_neighbors, region, k)
        return result

    def create_linking(self, malattia, i, emb):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_linking, malattia, i, emb)
        return result

    @staticmethod
    def _delete_graph(tx):
        query_string = "MATCH (n) DETACH DELETE n "
        tx.run(query_string)
        query_string = "MATCH (n) RETURN n "
        result = tx.run(query_string)
        return 0

    @staticmethod
    def _create_linking(tx, malattia, i, emb):

        meshidCode = malattia.split('\t')[1]
        malattia_name = malattia.split('\t')[0]
        for j in range(len(emb)):
            kwargs = {}
            query_string = ""
            if meshidCode == emb[j].get("number").get("value"):
                query_string += "MATCH(ad" + str(i) + ": Disease {name: $ad" + str(i) + "})\n"
                if emb[j].get("complLabel"):
                    query_string += "MERGE (cp" + str(i) + ": Disease {name: $cp" + str(i) + "})\n"
                    query_string += "CREATE (ad" + str(i) + ")-[:HAS_COMPLICATION]->(cp" + str(i) + ")\n"
                    kwargs["cp" + str(i)] = emb[j].get("complLabel").get("value")
                if emb[j].get("fieldLabel"):
                    query_string += "MERGE (sp" + str(i) + ": Specialization {name: $sp" + str(i) + "})\n"
                    query_string += "CREATE (ad" + str(i) + ")-[:HAS_SPECIALIZATION]->(sp" + str(i) + ")\n"
                    kwargs["sp" + str(i)] = emb[j].get("fieldLabel").get("value")
                if emb[j].get("trtLabel"):
                    query_string += "MERGE (tr" + str(i) + ": Treatment {name: $tr" + str(i) + "})\n"
                    query_string += "CREATE (ad" + str(i) + ")-[:HAS_TREATMNENT]->(tr" + str(i) + ")\n"
                    kwargs["tr" + str(i)] = emb[j].get("trtLabel").get("value")
                kwargs["ad" + str(i)] = malattia_name
                query_string += "RETURN 'linking effettuato'"
                tx.run(query_string, kwargs)

        return 0


def main():
    db = Neo4jConnector("bolt://localhost:7687", "neo4j", "12345")
    print("Connessione col database effettuata con successo.")

    with open('../input/disease_id.txt', encoding='utf-8-sig') as f:
        malattie_grafo = f.read().split('\n')

    print("Caricamento avviato.")

    with open('../input/dbpedia_disease_KB.json', mode='r') as f:
        emb1 = json.load(f)
    emb1 = emb1['results']['bindings']
    for i in range(len(malattie_grafo)):
            db.create_linking(malattie_grafo[i], i, emb1)

    print("Linking completato! Ora puoi giocare con Neo4j.")

if __name__ == "__main__":
    main()