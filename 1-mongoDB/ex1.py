from pymongo import MongoClient
import matplotlib.pyplot as plt
import csv
import json
import time

#read db credentials from config file
with open('config.json') as f:
    data = json.load(f)
    db_user = data['user']
    db_password = data['password']
    filename = data['filename']

#connect to the database
client: MongoClient = MongoClient('localhost', 27017, username=db_user, password=db_password)

db = client['exercise1']
coll_name = ['U', 'UA', 'A']
trash_coll = 'Trash'
year_col = 'Released_Year'
rating_col = 'IMDB_Rating'
certificate_col = 'Certificate'
all_films = 'All'

#save the films to the database based on the certificate
def save_films_to_db(filemame):
    with open(file=filemame, mode='r') as file:
        reader = csv.DictReader(file, delimiter=',')

        #create collections based on the certificate or clear the existing ones
        db[trash_coll].drop()
        db.create_collection(trash_coll)
        for name in coll_name:
            if name in db.list_collection_names():
                db[name].drop()
                db.create_collection(name)
        db[all_films].drop()
        db.create_collection(all_films)

        for row in reader:
            # exclude films with different certificate, year less than 1980 and rating less than 8
            if certificate_col in row and year_col in row and rating_col in row:
                try:
                    row[year_col] = int(row[year_col])
                    row[rating_col] = float(row[rating_col])
                except ValueError:
                    db[trash_coll].insert_one(row)
                    continue
                if row[certificate_col] in coll_name and row[year_col] >= 1980 and row[rating_col] >= 8:
                    # insert the film to the corresponding collection
                    db[row['Certificate']].insert_one(row)
                    # insert the film to the 'All' collection
                    db[all_films].insert_one(row)
                else:
                    db[trash_coll].insert_one(row)
            else:
                db[trash_coll].insert_one(row)

        print('Films are saved to the database')

def question1():
    """
    find all films published after 2015 grouped by genre.
    visualise an istogram with the number of films for each genre
    """
    print('Question 1: find all films published after 2015 grouped by genre')
    # db query
    now = time.time()
    pipeline = [
        {'$match': {year_col: {'$gt': 2015}, 'Genre': {'$exists': True}}},
        {"$project": {"Genre": {"$map": {"input": {"$split": ["$Genre", ","]}, "as": "g","in": {"$trim": { "input": "$$g" }}}}}},
        {'$unwind': '$Genre'},
        {'$group': {'_id': '$Genre', 'count': {'$sum': 1}}},
        {'$sort': {'count': -1}}
    ]
    genres = {}
    for coll in coll_name:
        part_genres = list(db[coll].aggregate(pipeline))
        # print(part_genres, '\n')
        for genre in part_genres:
            if genre['_id'] in genres:
                genres[genre['_id']] += genre['count']
            else:
                genres[genre['_id']] = genre['count']
    
    
    genres = dict(sorted(genres.items(), key=lambda item: item[1], reverse=True))
    elapsed = time.time() - now
    
    print(genres, '\n', elapsed, '\n')
    plt.figure(2, figsize=(9, 7))
    plt.bar(genres.keys(), genres.values())
    plt.xticks(rotation=45)
    plt.show()

def question2():
    """
    find the top 5 films for each genre based on the IMDB rating
    """
    print('Question 2: find the top 5 films for each genre based on the IMDB rating')
    # db query
    now = time.time()
    pipeline = [
        {
            "$project": {
                "Genre": {
                    "$map": {
                        "input": {"$split": ["$Genre", ","]},
                        "as": "g",
                        "in": {"$trim": {"input": "$$g"}}
                    }
                },
                "Series_Title": 1,
                "IMDB_Rating": 1
            }
        },
        {"$unwind": "$Genre"},
        {"$sort": {"IMDB_Rating": -1, "Series_Title": 1}},
        {
            "$group": {
                "_id": "$Genre",
                "films": {
                    "$push": {"Series_Title": "$Series_Title", "IMDB_Rating": "$IMDB_Rating"}
                }
            }
        },
        {
            "$project": {
                "_id": 0,
                "Genre": "$_id",
                "films": {"$slice": ["$films", 5]}
            }
        }
    ]
    top_films = {}
    for coll in coll_name:
        part_top_films = list(db[coll].aggregate(pipeline))
        for genre in part_top_films:
            if genre['Genre'] in top_films:
                top_films[genre['Genre']].extend(genre['films'])
            else:
                top_films[genre['Genre']] = genre['films']
    
    for genre in top_films:
        top_films[genre] = sorted(top_films[genre], key=lambda x: x['IMDB_Rating'], reverse=True)[:5]

    # order by genre
    top_films = dict(sorted(top_films.items(), key=lambda item: item[0]))
    print(top_films)
    file = open('question2_db.json', 'w')
    json.dump(top_films, file, indent=4)
    file.close()
    elapsed = time.time() - now
    print("DB way: ", elapsed, '\n')

def question3():
    """
    get the newest and oldest film for each genre
    """
    print('Question 3: get the newest and oldest film for each genre')
    # db query
    now = time.time()
    pipeline = [
        {
            "$project": {
                "Genre": {
                    "$map": {
                        "input": {"$split": ["$Genre", ","]},
                        "as": "g",
                        "in": {"$trim": {"input": "$$g"}}
                    }
                },
                "Series_Title": 1,
                "Released_Year": 1
            }
        },
        {"$unwind": "$Genre"},
        {"$sort": {"Released_Year": 1, "Series_Title": 1}},
        {
            "$group": {
                "_id": "$Genre",
                "newest": {"$last": {"Series_Title": "$Series_Title", "Released_Year": "$Released_Year"}},
                "oldest": {"$first": {"Series_Title": "$Series_Title", "Released_Year": "$Released_Year"}}
            }
        },
        {
            "$project": {
                "_id": 0,
                "Genre": "$_id",
                "newest": 1,
                "oldest": 1
            }
        }
    ]

    films = {}
    for coll in coll_name:
        part_films = list(db[coll].aggregate(pipeline))
        for genre in part_films:
            if genre['Genre'] in films:
                if genre['newest']['Released_Year'] > films[genre['Genre']]['newest']['Released_Year']:
                    films[genre['Genre']]['newest'] = genre['newest']
                if genre['oldest']['Released_Year'] < films[genre['Genre']]['oldest']['Released_Year']:
                    films[genre['Genre']]['oldest'] = genre['oldest']
            else:
                films[genre['Genre']] = {'newest': genre['newest'], 'oldest': genre['oldest']}
    
    # order by genre
    films = dict(sorted(films.items(), key=lambda item: item[0]))
    print(films)
    file = open('question3_db.json', 'w')
    json.dump(films, file, indent=4)
    file.close()
    elapsed = time.time() - now
    print("DB way: ", elapsed, '\n')

def question4():
    """
    get the oldest film with the highest rating
    """
    print('Question 4: get the oldest film with the highest rating')
    # db query
    now = time.time()
    pipeline = [
        {
            "$project": {
                "Series_Title": 1,
                "Released_Year": 1,
                "IMDB_Rating": 1
            }
        },
        {"$sort": {"Released_Year": 1, "IMDB_Rating": -1}},
        {"$limit": 1}
    ]
    oldest = {}
    for coll in coll_name:
        tmp = list(db[coll].aggregate(pipeline))[0]
        if 'Released_Year' in tmp and 'IMDB_Rating' in tmp:
            if 'Released_Year' not in oldest:
                oldest = tmp
            elif tmp['Released_Year'] < oldest['Released_Year']:
                oldest = tmp
            elif tmp['Released_Year'] == oldest['Released_Year'] and tmp['IMDB_Rating'] > oldest['IMDB_Rating']:
                oldest = tmp
    
    result = {'Series_Title': oldest['Series_Title'], 'Released_Year': oldest['Released_Year'], 'IMDB_Rating': oldest['IMDB_Rating']}
    print(result)
    file = open('question4_db.json', 'w')
    json.dump(result, file, indent=4)
    file.close()

    elapsed = time.time() - now
    print("DB way: ", elapsed, '\n')

def question5():
    """
    get the firsts 5 films with longest Runtime (specified in xxx min)
    """
    print('Question 5: get the firsts 5 films with longest Runtime')
    # db query
    now = time.time()
    pipeline = [
        {
            "$project": {
                "Series_Title": 1,
                "Runtime": {
                    "$toInt": {
                        "$arrayElemAt": [{"$split": ["$Runtime", " "]}, 0]
                    }
                }
            }
        },
        {"$sort": {"Runtime": -1}},
        {"$limit": 5}
    ]
    longest = []
    for coll in coll_name:
        tmp = list(db[coll].aggregate(pipeline))
        for film in tmp:
            if len(longest) < 5:
                longest.append(film)
            else:
                longest = sorted(longest, key=lambda x: x['Runtime'], reverse=True)
                if film['Runtime'] > longest[4]['Runtime']:
                    longest[4] = film
    
    result = []
    for film in longest:
        result.append({'Series_Title': film['Series_Title'], 'Runtime': film['Runtime']})

    elapsed = time.time() - now
    print(result)
    file = open('question5_db.json', 'w')
    json.dump(result, file, indent=4)
    file.close()
    print("DB way: ", elapsed, '\n')

def question6():
    """
    get the gross income for each genre 
    visualise it with an histogram
    """
    print('Question 6: get the gross income for each genre')
    # db query
    now = time.time()
    pipeline = [
    {
        "$project": {
            "Genre": {
                "$map": {
                    "input": {"$split": ["$Genre", ","]},
                    "as": "g",
                    "in": {"$trim": {"input": "$$g"}}
                }
            },
            "Gross": {
                "$toInt": {
                    "$replaceAll": {
                        "input": {
                            "$cond": {
                                "if": {"$eq": ["$Gross", ""]},
                                "then": "0",
                                "else": "$Gross"
                            }
                        },
                        "find": ",",
                        "replacement": ""
                    }
                }
            }
        }
    },
    {"$unwind": "$Genre"},
    {
        "$group": {
            "_id": "$Genre",
            "total": {"$sum": "$Gross"}
        }
    },
    {"$sort": {"total": -1}}
    ];
    genres = {}
    for coll in coll_name:
        part_genres = list(db[coll].aggregate(pipeline))
        for genre in part_genres:
            if genre['_id'] in genres:
                genres[genre['_id']] += genre['total']
            else:
                genres[genre['_id']] = genre['total']

    genres = dict(sorted(genres.items(), key=lambda item: item[1], reverse=True))
    elapsed = time.time() - now
    
    print(genres, '\n', elapsed, '\n')
    plt.figure(2, figsize=(9, 7))
    plt.bar(genres.keys(), genres.values())
    plt.xticks(rotation=45)
    plt.show()
                            

if __name__ == '__main__':
    import sys
    command = ''
    while command != 'exit':
        if len(sys.argv) < 2:
            command = input('Type the question number: [1-6] or "save" to save the films to the database\n')
        else:
            command = sys.argv[1]
        if command == 'save':
            save_films_to_db(filename)
        else:
            questions = {
                '1': question1,
                '2': question2,
                '3': question3,
                '4': question4,
                '5': question5,
                '6': question6
            }
            if command in questions and command != 'save':
                questions[command]()
            else:
                print('exiting...')
                break
    client.close()