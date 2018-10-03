from py2neo import Graph,Node,Relationship,NodeMatcher
db = Graph("bolt://localhost:7687", password="123")

# tx = db.begin()
# matcher = NodeMatcher(db)
# node = Node("Genres", name="Action", )
# print(matcher.match("Genres", name="Action"))
# movie_exists = matcher.match("Movie", id=1, name="Toy Story (1995)").first()
# print(movie_exists)


# Replace "::" to ","
# with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings.csv', 'r', encoding='UTF-8') as f:
#     data = f.readlines()
#     f.close()
#
# for index, replaced_data in enumerate(data):
#     data[index] = replaced_data.replace("::", ",")
# with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings.csv', 'w', encoding='UTF-8') as f:
#     f.writelines(data)
#     f.close()

query = "USING PERIODIC COMMIT 50000 " \
        "LOAD CSV WITH HEADERS FROM 'file:///ratings.csv' " \
        "as line FIELDTERMINATOR ',' " \
        "MATCH (m:Movie{id:toInteger(line.movie_id)}) " \
        "MERGE (u:User{id:toInteger(line.user_id)}) " \
        "MERGE (u)-[:RATES {rating:line.rating}]->(m)"
db.run(query)
