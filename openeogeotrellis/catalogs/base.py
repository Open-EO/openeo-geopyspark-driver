import logging
from abc import abstractclassmethod, abstractproperty
from enum import Enum


class CatalogConstantsBase:
    
    @abstractproperty
    def missionSentinel2(self): pass

    @abstractproperty
    def level1C(self): pass

    @abstractproperty
    def level2A(self): pass

    def getMissionString(self,mission): return getattr(self,mission)
    def getLevelString(self,level): return getattr(self,level)
    
    
class CatalogStatus(Enum):
    NOT_FOUND=1
    AVAILABLE=2
    ORDERABLE=3
    

class CatalogEntryBase:

    @abstractclassmethod
    def __init__(self, product_id, s3_bucket, s3_key): pass

    @abstractclassmethod
    def __str__(self): pass

    @abstractclassmethod
    def getProductId(self): pass

    @abstractclassmethod
    def getS3Bucket(self): pass
    
    @abstractclassmethod
    def getS3Key(self): pass
    
    @abstractclassmethod
    def getTileId(self): pass
    
    @abstractclassmethod
    def getStatus(self): pass
    
    @abstractclassmethod
    def getFileRelPath(self,s3fileutil,band,resolution): pass

    def getTileInfo(self, ):
        """
        Returns some product metadata
        """
        pass

    
    def getFileAbsPath(self,s3fileutil,band,resolution):
        return '/'.join([self.getS3Bucket(),self.getS3Key(),self.getFileRelPath(s3fileutil, band, resolution)])



class CatalogClientBase:

    def __init__(self, mission, level):
        self.mission = mission
        self.level = level
        self.logger = logging.getLogger(self.__class__.__name__)

    @abstractclassmethod
    def catalogEntryFromProductId(self,product_id): pass  

    @abstractclassmethod
    def query(self, start_date, end_date, 
              tile_ids=None,
              ulx=-180, uly=90, brx=180, bry=-90,
              cldPrcnt=100.): pass
    
    @abstractclassmethod
    def count(self, start_date, end_date,
              tile_ids=None,
              ulx=-180, uly=90, brx=180, bry=-90,
              cldPrcnt=100.): pass
