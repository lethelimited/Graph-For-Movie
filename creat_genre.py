from py2neo import Graph, Node, NodeMatcher


def create_genres():
    tx = db.begin()
    matcher = NodeMatcher(db)
    genres_list = {"Action", "Adventure", "Animation",
                   "Children", "Comedy", "Crime",
                   "Documentary", "Drama", "Fantasy",
                   "Film-Noir", "Horror", "Musical",
                   "Mystery", "Romance", "Sci-Fi",
                   "Thriller", "War", "Western"
                   }
    for index, m in enumerate(genres_list, start=1):
        print(index)
        node = Node("Genres", name=m)
        if matcher.match("Genres", name=m).first() is None:
            tx.create(node)

    tx.commit()


db = Graph("bolt://localhost:7687", password="123")
create_genres()
