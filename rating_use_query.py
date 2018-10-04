from py2neo import Graph, Relationship, NodeMatcher, Node
import sys
db = Graph("bolt://localhost:7687", password="123")
# tx = db.begin()
# tx.commit()


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


def insert_tags():
    # UserID::MovieID::Tag::Timestamp
    with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/tags.dat', 'r', encoding='UTF-8') as f:
        text = f.readlines()
        f.close()
    matcher = NodeMatcher(db)
    tx = db.begin()
    for data in text:
        split_data = data.split("::")

        user_id = int(split_data[0])
        movie_id = int(split_data[1])
        tag = split_data[2]

        user = matcher.match("User", id=user_id).first()
        movie = matcher.match("Movie", id=movie_id).first()

        if user and movie:
            relationship = Relationship(user, "TAG", movie, tag=tag)
            tx.create(relationship)
            sys.stdout.write('{}\r' + str(user_id))
    tx.commit()


def query_insert_rating():
    # import/rating.csv
    query = "USING PERIODIC COMMIT 10000 " \
            "LOAD CSV WITH HEADERS FROM 'file:///ratings.csv' " \
            "as line FIELDTERMINATOR ',' " \
            "MATCH (m:Movie{id:toInteger(line.movie_id)}) " \
            "MERGE (u:User{id:toInteger(line.user_id)}) " \
            "MERGE (u)-[:RATES {rating:line.rating}]->(m)"
    db.run(query)


insert_tags()
