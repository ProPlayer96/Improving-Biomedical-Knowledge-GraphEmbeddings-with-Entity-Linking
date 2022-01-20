import neo4j
import json

from neo4j import GraphDatabase

class Neo4jConnector(object):

    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self._driver.close()

    def clean_database(self):
        with self._driver.session() as session:
            result = session.write_transaction(self._delete_graph)
        return result

    def top_k_neighbors(self,region,k):
        with self._driver.session() as session:
            result = session.read_transaction(self._top_k_neighbors, region,k)
        return result

    def create_paziente(self, cod):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_paziente, cod)
        return result

    def create_visita(self, codpaz, visita):
        with self._driver.session() as session:
            result = session.write_transaction(self._create_visita, codpaz, visita)
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
        query_string = "CREATE (p: Patient) \n" \
                       "SET p.id = $id \n" \
                       "RETURN 'node created:' + id(p)"
        kwargs = {"id": cod}
        tx.run(query_string, kwargs)
        return 0

    @staticmethod
    def _create_visita(tx, codpaz, visita):

        has_anamnesi = True if "anamnesi" in visita.keys() else False
        has_reparto = True if "reparto" in visita.keys() else False
        has_diagnosi = True if "diagnosi" in visita.keys() else False
        has_segni = True if "segni" in visita.keys() else False

        kwargs = {}
        query_string = ""

        query_string += "MATCH (p: Patient {id: $codpaz}) \n"
        query_string += "CREATE (v: Visit {date: $date}) \n"

        query_string += "CREATE (p)-[:HAS_DONE]->(v) \n"
        kwargs['date'] = visita['data']
        kwargs['codpaz'] = codpaz

        if has_reparto is True:
            query_string += "SET v.department = $department \n"
            kwargs['department'] = visita['reparto']

        if has_anamnesi is True:
            query_string += "CREATE (v)-[:HAS_ANAMNESIS]->(a: Anamnesis) \n"

            entities = retrieve_entities(visita['anamnesi']['testo'], visita['anamnesi']['etichette'])

            for i in range(len(entities['Disease'])):
                disease = entities['Disease'][i]
                query_string += "MERGE (ad" + str(i)+ ": Disease {name: $ad" + str(i)+"})\n"
                query_string += "CREATE (a)-[:HAS_A_DISEASE]->(ad" + str(i)+ ")\n"
                kwargs["ad" + str(i)] = disease.lower()

            for i in range(len(entities['Symptom'])):
                symptom = entities['Symptom'][i]
                query_string += "MERGE (as" + str(i)+ ": Symptom {name: $as" + str(i)+"})\n"
                query_string += "CREATE (a)-[:HAS_SYMPTOM]->(as" + str(i)+ ")\n"
                kwargs["as" + str(i)] = symptom.lower()

        if has_diagnosi is True:
            query_string += "CREATE (v)-[:HAS_DIAGNOSIS]->(d: Diagnosis) \n"

            entities = retrieve_entities(visita['diagnosi']['testo'], visita['diagnosi']['etichette'])

            for i in range(len(entities['Disease'])):
                disease = entities['Disease'][i]
                query_string += "MERGE (dd" + str(i)+ ": Disease {name: $dd" + str(i)+"})\n"
                query_string += "CREATE (d)-[:HAS_D_DISEASE]->(dd" + str(i)+ ")\n"
                kwargs["dd" + str(i)] = disease.lower()
				
		if has_segni is True:
            query_string += "CREATE (e)-[:HAS_TESTED_SYMPTOMS]->(t: TestedSymptoms) \n"

            entities = retrieve_entities(visita['segni']['testo'], visita['segni']['etichette'])

            for i in range(len(entities['Symptom'])):
                symptom = entities['Symptom'][i]
                query_string += "MERGE (ts" + str(i)+ ": Symptom {name: $ts" + str(i)+"})\n"
                query_string += "CREATE (t)-[:HAS_SYMPTOM]->(ts" + str(i)+ ")\n"
                kwargs["ts" + str(i)] = symptom.lower()

        query_string += "RETURN 'visita inserita'"

        tx.run(query_string, kwargs)
        return 0

def retrieve_entities(testo, etichette):
    entities = {'Disease': [], 'Symptom': []}

    i=0
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

    db.clean_database()
    print("Cleaning del database effettuato.")

    print("Caricamento avviato.")
    N = len(data)

    i = 0
    for person in data:
        db.create_paziente(person['codice'])
		
        for visita in person['visite']:
            db.create_visita(person['codice'], visita)
        i+=1
        print(i, " / ", N, " (", person['codice'], ")")

    print("Caricamento completato! Ora puoi giocare con Neo4j.")

if __name__ == "__main__":
    main()