
'''
Date : 14/Aug/2020
Author : Jugal Bhatt
Movies Genre Ratings
'''
class DataExtract:
    "Data Extract"
    
    def __init__(self,spark):
        self.spark = spark
    
    '''
    Join Users/Movies/Ratings Tables
    '''     
    def __getPopMovies(self,movies,ratings,users):
        return movies.join(ratings, movies.movieid == ratings.movie_id, how='inner').join(users, ratings.user_id == users.userid,how='inner')
            
    
    