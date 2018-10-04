from py2neo import Graph,Node,Relationship,NodeMatcher
import sys,timeit
# db = Graph("bolt://localhost:7687", password="123")

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

# start = timeit.default_timer()
# main()
# end = timeit.default_timer()
# print(end - start)

start = timeit.default_timer()
with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings_import.csv', 'r', encoding='UTF-8') as f:
        data = f.readlines()
        f.close()
for index, replaced_data in enumerate(data):
        split_data = replaced_data.split(",")
        data[index] = split_data[0]+","+split_data[2]+","+split_data[1]+",RATINGS\n"
        sys.stdout.write("\r"+str(index))
with open('/Users/Lim/Documents/DAnalyticsWorkspace/ml-10M100K/ratings_import.csv', 'w', encoding='UTF-8') as f:
        f.writelines(data)
        f.close()
end = timeit.default_timer()
print("\n")
print(end - start)
