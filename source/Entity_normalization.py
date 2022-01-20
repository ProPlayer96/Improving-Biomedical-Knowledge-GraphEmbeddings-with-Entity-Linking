import neo4j
import json
import Levenshtein
from neo4j import GraphDatabase
import pandas as pd
import numpy as np
import torch
from transformers import BertTokenizer, BertModel
from scipy.spatial.distance import cosine


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

    def create_linking(self, codpaz, visita, emb):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_linking, codpaz, visita, emb)
        return result

    @staticmethod
    def _delete_graph(tx):
        query_string = "MATCH (n) DETACH DELETE n "
        tx.run(query_string)
        query_string = "MATCH (n) RETURN n "
        result = tx.run(query_string)
        return 0

    @staticmethod
    def _create_linking(tx, codpaz, visita, emb):
        has_anamnesi = True if "anamnesi" in visita.keys() else False
        has_reparto = True if "reparto" in visita.keys() else False
        has_diagnosi = True if "diagnosi" in visita.keys() else False
        has_segni = True if "segni" in visita.keys() else False


        if has_anamnesi is True:
            entities = retrieve_entities(visita['anamnesi']['testo'], visita['anamnesi']['etichette'])
            for i in range(len(entities['Disease'])):
                disease_win = entities['Disease'][i]
                print(disease_win)

                for j in range(len(emb)):
                    if disease_win.lower() == emb[j]["mention"].lower():
                        kwargs = {}
                        query_string = ""
                        query_string += "MATCH (ad" + str(i) + ": Disease {name: $ad" + str(i) +"}) \n"
                        query_string += "SET ad" + str(i) + ".name = $name" + str(i) +"\n"
                        query_string += "SET ad" + str(i) + ".meshIDcode = $meshIDcode" + str(i) + "\n"
                        query_string += "SET ad" + str(i) + ".descr = $descr" + str(i) + "\n"

                        kwargs["ad" + str(i)] = disease_win.lower()
                        kwargs["name" + str(i)] = emb[j]["entity"]
                        kwargs["meshIDcode" + str(i)] = emb[j]["entity_number"]
                        kwargs["descr" + str(i)] = emb[j]["entity_descr"]

                        if emb[j]["entity_spec"] != 'No description':
                            query_string += "SET ad" + str(i) + ".specialization = $spec" + str(i) + "\n"
                            kwargs["spec" + str(i)] = emb[j]["entity_spec"]
                        
                        tx.run(query_string, kwargs)
                        break

            for i in range(len(entities['Symptom'])):
                symptom_win = entities['Symptom'][i]

                for j in range(len(emb)):
                    if symptom_win.lower() == emb[j]["mention"].lower():
                        kwargs = {}
                        query_string = ""
                        query_string += "MATCH (as" + str(i) + ": Symptom {name: $as" + str(i) +"}) \n"
                        query_string += "SET as" + str(i) + ".name = $as_name" + str(i) +"\n"
                        query_string += "SET as" + str(i) + ".meshIDcode = $as_meshIDcode" + str(i) + "\n"
                        query_string += "SET as" + str(i) + ".descr = $as_descr" + str(i) + "\n"

                        kwargs["as" + str(i)] = symptom_win.lower()
                        kwargs["as_name" + str(i)] = emb[j]["entity"]
                        kwargs["as_meshIDcode" + str(i)] = emb[j]["entity_number"]
                        kwargs["as_descr" + str(i)] = emb[j]["entity_descr"]

                        if emb[j]["entity_spec"] != 'No description':
                            query_string += "SET as" + str(i) + ".specialization = $as_spec" + str(i) + "\n"
                            kwargs["as_spec" + str(i)] = emb[j]["entity_spec"]
                        
                        tx.run(query_string, kwargs)
                        break

        if has_diagnosi is True:
            entities = retrieve_entities(visita['diagnosi']['testo'], visita['diagnosi']['etichette'])
            for i in range(len(entities['Disease'])):
                disease_win = entities['Disease'][i]
                print(disease_win)

                for j in range(len(emb)):
                    if disease_win.lower() == emb[j]["mention"].lower():
                        kwargs = {}
                        query_string = ""
                        query_string += "MATCH (dd" + str(i) + ": Disease {name: $dd" + str(i) +"}) \n"
                        query_string += "SET dd" + str(i) + ".name = $dd_name" + str(i) +"\n"
                        query_string += "SET dd" + str(i) + ".meshIDcode = $dd_meshIDcode" + str(i) + "\n"
                        query_string += "SET dd" + str(i) + ".descr = $dd_descr" + str(i) + "\n"

                        kwargs["dd" + str(i)] = disease_win.lower()
                        kwargs["dd_name" + str(i)] = emb[j]["entity"]
                        kwargs["dd_meshIDcode" + str(i)] = emb[j]["entity_number"]
                        kwargs["dd_descr" + str(i)] = emb[j]["entity_descr"]

                        if emb[j]["entity_spec"] != 'No description':
                            query_string += "SET dd" + str(i) + ".specialization = $dd_spec" + str(i) + "\n"
                            kwargs["dd_spec" + str(i)] = emb[j]["entity_spec"]
                        
                        tx.run(query_string, kwargs)
                        break

        return 0


def retrieve_entities(testo, etichette):
    entities = {'Disease': [], 'Symptom': []}

    i = 0
    while i < len(testo):
        if len(etichette[i].split("B-")) == 2:
            entity = []
            entity_type = etichette[i].split("B-")[-1]
            entity.append(testo[i])
            i = i + 1
            if i < len(testo):
                while (len(etichette[i].split("I-")) == 2):
                    entity.append(testo[i])
                    i = i + 1
                    if i == len(testo):
                        break
            entities[entity_type].append(" ".join(entity))
        i = i + 1

    return entities


def main():
    db = Neo4jConnector("bolt://localhost:7687", "neo4j", "12345")
    print("Connessione col database effettuata con successo.")

    with open('../input/graph_data.json') as json_file:
        data = json.load(json_file)
    print("Il file da importare è stato letto.")


    print("Caricamento avviato.")
    N = len(data)

    with open('../input/prediction.json', mode='r') as f:
        emb1 = json.load(f)


    i = 0
    for person in data:

        for visita in person['visite']:
            db.create_linking(person['codice'], visita, emb1)
        i += 1
        print(i, " / ", N, " (", person['codice'], ")")

    print("Normalizzazione completata! Ora puoi giocare con Neo4j.")

if __name__ == "__main__":
    main()