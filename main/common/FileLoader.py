'''
Date : 14/Aug/2020
Author : Jugal Bhatt
Movies Genre Ratings
'''
class FileLoader:
    "File Loader"
    
    def __init__(self,spark):
        self.spark = spark
   
   '''
   Dynamically loads any file format
   '''     
    def __getFiles(self,formats,schemas,delimit,path):
        return self.spark.read.format(formats).schema(schemas).options(delimiter=delimit).load(path)
