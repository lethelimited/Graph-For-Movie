from py2neo import Graph


def ready_to_load():
    # Replace "::" to ","
    with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings.csv', 'r', encoding='UTF-8') as f:
        data = f.readlines()
        f.close()

    for index, replaced_data in enumerate(data):
        data[index] = replaced_data.replace("::", ",")
    with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings.csv', 'w', encoding='UTF-8') as f:
        f.writelines(data)
        f.close()


db = Graph("bolt://localhost:7687", password="123")
# import/rating.csv
query = "USING PERIODIC COMMIT 50000 " \
        "LOAD CSV WITH HEADERS FROM 'file:///ratings.csv' " \
        "as line FIELDTERMINATOR ',' " \
        "MATCH (m:Movie{id:toInteger(line.movie_id)}) " \
        "MERGE (u:User{id:toInteger(line.user_id)}) " \
        "MERGE (u)-[:RATES {rating:line.rating}]->(m)"
db.run(query)