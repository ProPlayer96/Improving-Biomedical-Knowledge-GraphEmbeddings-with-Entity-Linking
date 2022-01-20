import pandas as pd
import numpy as np
import torch
from transformers import BertTokenizer, BertModel
from scipy.spatial.distance import cosine
from neo4j import GraphDatabase


#RECUPERO ENTITA' RIGA PER RIGA, quindi nel main cicleremo sulle righe del DF
#testo = row['Diagnosi']
#etichette = row['prediction_diagnosi']
def retrieve_entities(testo,etichette):
    entities = {'Disease': [], 'Symptom': []}

    i=0 #parola nella frase
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


class Neo4jConnection:

  def __init__(self, uri, user, password):
    self._driver = GraphDatabase.driver(uri, auth=(user, password), encrypted=False)

  def close(self):
    if self.__driver is not None:
      self.__driver.close()

  def clean_database(self):
    with self._driver.session() as session:
      result = session.write_transaction(self._delete_graph)
    return result

  def top_k_neighbors(self, region, k):
    with self._driver.session() as session:
      result = session.read_transaction(self._top_k_neighbors, region, k)
    return result

  def create_paziente(self, cod):
    with self._driver.session() as session:
      result = session.write_transaction(self._create_paziente, cod)
    return result

  def create_visita(self, codpaz, visita, anam, pred_anam, diagn, pred_diag, farm_a, farm_d):
      with self._driver.session() as session:
          result = session.write_transaction(self._create_visita, codpaz, visita, anam, pred_anam, diagn,
                                             pred_diag, farm_a,
                                             farm_d)
      return result

  @staticmethod
  def _delete_graph(tx):
      query_string = "MATCH (n) DETACH DELETE n "
      tx.run(query_string)
      query_string = "MATCH (n) RETURN n "
      result = tx.run(query_string)
      return 0

  @staticmethod
  def _create_paziente(tx, cod):
      query_string = "MERGE (p: Patient {id: $id}) \n" \
                     "ON CREATE SET p.id = $id \n" \
                     "RETURN 'node created:' + id(p)"
      kwargs = {"id": cod}
      tx.run(query_string, kwargs)
      return 0


  @staticmethod
  def _create_visita(tx, codpaz, visita, anam, pred_anam, diagn, pred_diag, farm_a, farm_d):

      kwargs = {}
      query_string = ""

      query_string += "MATCH (p: Patient {id: $codpaz}) \n"
      query_string += "CREATE (v: Visit {date: $date}) \n"

      query_string += "CREATE (p)-[:HAS_DONE]->(v) \n"
      kwargs['date'] = visita[0]
      kwargs['codpaz'] = codpaz

      if not pd.isnull(visita[1]):
          query_string += "SET v.cod = $cod \n"
          kwargs['cod'] = visita[1]

      if anam:
          query_string += "CREATE (v)-[:HAS_ANAMNESIS]->(a: Anamnesis) \n"

          entities = retrieve_entities(anam, pred_anam)

          for i in range(len(entities['Disease'])):
		  
              disease_win = entities['Disease'][i]
              query_string += "MERGE (ad" + str(i) + ": Disease {name: $ad" + str(i) + "})\n"
              query_string += "CREATE (a)-[:HAS_A_DISEASE]->(ad" + str(i) + ")\n"             
              kwargs["ad" + str(i)] = disease.lower()
        

          for i in range(len(entities['Symptom'])):
		  
              symptom_win = entities['Symptom'][i]             
              query_string += "MERGE (as" + str(i) + ": Symptom {name: $as" + str(i) + "})\n"
              query_string += "CREATE (a)-[:HAS_SYMPTOM]->(as" + str(i) + ")\n"
              
              kwargs["as" + str(i)] = symptom.lower()

          for i in range(9):
              if not pd.isnull(farm_a[i]):
                  query_string += "MERGE (fa" + str(i) + ": Drug {name: $name" + str(i) +"}) \n"
                  query_string += "CREATE (a)-[:HAS_DRUGS]->(fa" + str(i) + ") \n"
                  kwargs['name' + str(i)] = farm_a[i]

      if diagn:
          query_string += "CREATE (v)-[:HAS_DIAGNOSIS]->(d: Diagnosis) \n"

          entities = retrieve_entities(diagn, pred_diag)

          for i in range(len(entities['Disease'])):
              disease_win = entities['Disease'][i]
              
              query_string += "MERGE (dd" + str(i) + ": Disease {name: $dd" + str(i) + "})\n"
              query_string += "CREATE (d)-[:HAS_DISEASE]->(dd" + str(i) + ")\n"
              
              kwargs["dd" + str(i)] = disease.lower()
              

          for i in range(9):
              if not pd.isnull(farm_d[i]):
                  query_string += "MERGE (fd" + str(i) + ": Drug {name: $name" + str(i) + "}) \n"
                  query_string += "CREATE (d)-[:HAS_DRUGS]->(fd" + str(i) + ") \n"
                  kwargs['name' + str(i)] = farm_d[i]



      query_string += "RETURN 'visita inserita'"
      tx.run(query_string, kwargs)
      return ris


def main():

    db = Neo4jConnection("bolt://127.0.0.1:7687", "neo4j", "12345")

    print("Connessione col database effettuata con successo.")

    #db.clean_database()
    print("Cleaning del database effettuato.")


    df_ecg = pd.read_pickle("your_path\\dataset_df_ecg.pkl")

    print("Caricamento avviato.")
    i = 0
    for index, row in df_ecg.iterrows():
        row['VISITA'][0] = row['VISITA'][0].to_pydatetime()
        db.create_paziente(row['CODPAZ'])

        db.create_visita(row['CODPAZ'], row['VISITA'], row['Anamnesi'], row['prediction_anamnesi'],
                         row['Diagnosi'],
                         row['prediction_diagnosi'], row['FARMACI_ANAMNESI'], row['FARMACI_DIAGNOSI'])
        i += 1
        print("visita", i, " - ", len(df_ecg), "inserita")
    print("Caricamento completato! Ora puoi giocare con Neo4j.")



if __name__ == "__main__":
    main()