from py2neo import Graph, Node, Relationship, NodeMatcher
import sys
# head -3 movies.dat
# MovieID::Title::Genres
# 1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy
# 2::Jumanji (1995)::Adventure|Children|Fantasy
# 3::Grumpier Old Men (1995)::Comedy|Romance
#
# head -3 ratings.dat
# UserID::MovieID::Rating::Timestamp
# 1::122::5::838985046
# 1::185::5::838983525
# 1::231::5::838983392
#
# head -3 tags.dat
# UserID::MovieID::Tag::Timestamp
# 15::4973::excellent!::1215184630
# 20::1747::politics::1188263867
# 20::1747::satire::1188263867


def split_movie(record):
    split_text = record.split("::")
    m_id = split_text[0]
    m_title = split_text[1]
    m_genre = split_text[2].split("|")
    return m_id, m_title, m_genre


# print(split_movie("1::Toy Story (1995)::Adventure|Animation|Children|Comedy|Fantasy"))
def insert_movie_into_graph(m_id, m_title, m_genres):

    matcher = NodeMatcher(db)
    movie = Node("Movie", id=m_id, name=m_title)
    movie_match = matcher.match("Movie", id=m_id, name=m_title).first()
    sys.stdout.write('{}\r' + str(m_id))
    if not movie_match:
        tx.create(movie)
        for index, m in enumerate(m_genres,start=1):
            genres = matcher.match("Genres", name=m.replace("\n", "")).first()
            # print(index)
            if genres:
                tx.create(Relationship(movie, 'OF_TYPE', genres))


def open_file(filename):
    with open(filename, 'r', encoding='UTF-8') as f:
        texts = f.readlines()
    return texts


def start_movie():
    records = open_file("/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/movies.dat")
    # print(records)
    for record in records:
        sp_record = split_movie(record)
        insert_movie_into_graph(int(sp_record[0]), sp_record[1], sp_record[2])


db = Graph("bolt://localhost:7687", password="123")
tx = db.begin()
start_movie()
tx.commit()
