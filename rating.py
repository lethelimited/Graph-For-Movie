import timeit
import sys

from py2neo import Graph, Node, Relationship, NodeMatcher

user_map = {}
graph = Graph("bolt://localhost:7687", password="123")


def open_file(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        texts = f.readlines()
    return texts


def insert_rating(node_user, rating, node_movie):
    # rel = Relationship(node_id, 'RATES', node_movie, rating=rating)
    graph.create(Relationship(node_user, 'RATES', node_movie, rating=rating))


def main():
    # Can I keep the user_node

    text_generator = open_file('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings.dat')
    graph.begin()
    matcher = NodeMatcher(graph)
    # for text in text_generator:
    useriddd = 0
    for data in text_generator:
        if data:
            split_data = data.split('::')
            # split_data[3] = split_data[3].replace("\n", "")
            for x in range(4):
                if x == 2:
                    split_data[x] = float(split_data[x])
                split_data[x] = int(split_data[x])

        user_id, movie_id, ratings = split_data[0:3]

        movie_node = matcher.match("Movie", id=movie_id).first()
        user = matcher.match("User", id=user_id).first()
        if not user:
            user_node = Node("User", id=user_id)
            insert_rating(user_node, ratings, movie_node)
        else:
            insert_rating(user, ratings, movie_node)
        if user_id > useriddd:
            useriddd = user_id
            sys.stdout.write('{}\r'+str(useriddd))
        # insert_rating(user_node, ratings, matcher.match("Movie", id=movie_id).first())
    graph.commit()


if __name__ == '__main__':
    # 70 ms
    main()
