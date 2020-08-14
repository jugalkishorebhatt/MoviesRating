import logging
import yaml
import traceback
from pyspark.sql.types import StructType, StructField
from pyspark.sql.types import DoubleType,LongType , IntegerType, StringType
from pyspark.sql import functions as F
from config import SparkConfig
from common import FileLoader
from actions import DataExtract


'''
Date : 14/Aug/2020
Author : Jugal Bhatt
Movies Genre Ratings
'''
class App:
    'App Controller'
    
    def __init__(self):
        pass

if __name__ == "__main__":
    "Main"
    logging.basicConfig(filename = './logname.log', 
                            filemode = 'a', 
                            format = '[[%(filename)s:%(lineno)s :] %(asctime)s, %(msecs)d %(name)s %(levelname)s %(message)s', 
                            datefmt = '%H:%M:%S', 
                            level = logging.DEBUG)
    
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)

    logger.info('Extract Movie Data')
    
    try:
        with open('/home/dwarika/Learning/Momenton/main/config.yml', 'r') as ymlfile:
            cfg = yaml.load(ymlfile)
    except Exception as e:
        logger.error("Main Method, config file", traceback.print_exc())
        
    spark = SparkConfig.SparkConfig()._SparkConfig__configurations();
    logger.info("Spark Config Initialized")
    
    movSchema = StructType([
    StructField("movieid", IntegerType()),
    StructField("movie_title_year", StringType()),
    StructField("genres", StringType())
])
    
    movies = FileLoader.FileLoader(spark)._FileLoader__getFiles(cfg['movies']['format'],
                                                                movSchema,
                                                                cfg['movies']['delimit'],
                                                                cfg['movies']['path'])
    movies.show(truncate=False)
    logger.info("Movies Data loaded")
  
    ratSchema = StructType([
    StructField("user_id", IntegerType()),
    StructField("movie_id", IntegerType()),
    StructField("rating", IntegerType()),
    StructField("rating_tmp", LongType())
])  
    ratings = FileLoader.FileLoader(spark)._FileLoader__getFiles(cfg['rating']['format'],
                                                                 ratSchema,
                                                                 cfg['rating']['delimit'],
                                                                 cfg['rating']['path'])
    ratings.show()
    logger.info("Rating Data loaded")
    
    usrSchema = StructType([
    StructField("userid", IntegerType()),
    StructField("twitterid", IntegerType())
])  
    users = FileLoader.FileLoader(spark)._FileLoader__getFiles(cfg['user']['format'],
                                                               usrSchema,
                                                               cfg['user']['delimit'],
                                                               cfg['user']['path'])
    users.show()
    logger.info("Users Data loaded")
    
    popMovGen = DataExtract.DataExtract(spark)._DataExtract__getPopMovies(movies,ratings,users)
    popMovGen.show()
    popMovGenOrd = popMovGen.select("movieid","rating","genres",F.year(F.from_unixtime(F.col("rating_tmp")/1000)).alias('rating_year'))
    #popMovGenOrd = popMovGen.select("movieid","rating","genres",F.year(F.from_unixtime(F.col("rating_tmp")/1000)).alias('rating_year') ,(F.year(F.from_unixtime(F.col("rating_tmp")/1000)) - 10).alias('last_decade'))#.orderBy(F.col('rating').desc())
    #popMovGenOrd.agg({'last_decade' : 'max','rating_year' : 'max'}).show()
    logger.info("Popular Movie Genres")
    
    movByYear =  popMovGenOrd.groupBy('genres','rating','rating_year')
    movByYear.sum('rating').withColumnRenamed('sum(rating)','sumRate').orderBy(F.col('sumRate').desc()).show()
    logger.info("Popular Movie Genres By Year")