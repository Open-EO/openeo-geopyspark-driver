'''
Created on Dec 9, 2019

@author: banyait
'''
import logging
import requests
import json
import os
import datetime
from openeogeotrellis.catalogs.base import CatalogStatus


class CreoOrder(object):
    '''
    classdocs
    '''


    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        
    def _handle_request(self,session,request):
        pr=request.prepare()
        self.logger.debug("--- request")
        self.logger.debug(pr.url)
        self.logger.debug(pr.headers)
        self.logger.debug(pr.body)
        resp=session.send(pr)
        self.logger.debug("--- response: "+str(resp.status_code))
        if resp.status_code<200 or resp.status_code>=300:
            self.logger.debug(resp.content)
            raise Exception("Error executing request in creo ordering module: "+str(resp.status_code)+': '+str(resp.content))
        return resp
    
    def _build_session(self):
        s=requests.Session()
        return s

    def _get_auth_token(self,session):
        self.logger.debug("*** AUTHENTICATING ***")
        usr=os.getenv("OS_USERNAME", "<OS_USERNAME_NOT_FOUND>")
        pwd=os.getenv("OS_PASSWORD", "<OS_PASSWORD_NOT_FOUND>")
        request=requests.Request(
            "POST",
            url="https://auth.creodias.eu/auth/realms/DIAS/protocol/openid-connect/token",
            data={
                'client_id':'CLOUDFERRO_PUBLIC',
                'username':usr,
                'password':pwd,
                'grant_type':'password' 
            }
        )
        respjson= self._handle_request(session, request).json()
        self.logger.debug(json.dumps(respjson,indent=2))
        return respjson['access_token']


    def _send_order(self,session,token, catalog_entries, tag):
        self.logger.debug("*** ORDERS PRODUCTS ***")
        if catalog_entries is None: return ''
        orderlist=list(map(lambda j: j.getProductId(), filter(lambda i: i.getStatus()==CatalogStatus.ORDERABLE, catalog_entries)))
#        orderlist=list(map(lambda j: j.getProductId(), catalog_entries))
        self.logger.info("Ordering products: "+str(len(orderlist)))
        if len(orderlist)==0: return ''
        pl={
            "order_name": "order_"+tag+'_'+datetime.datetime.now().strftime("%Y/%m/%d-%H:%M:%S"),
            "priority": 0,
#            "destination": "order_destination",
#            "destination": "unit-tests",
            "identifier_list": orderlist,
            "processor": "sen2cor"
#            "processor": "download"
        }
        request=requests.Request(
            "POST",
            url="https://finder.creodias.eu/api/order/",
            headers={
                'Content-Type':'application/json',
                'Keycloak-Token':token 
            },
            json=pl
        )
        respjson= self._handle_request(session, request).json()
        self.logger.info("Order id: "+str(respjson['id']))
        self.logger.debug(json.dumps(respjson,indent=2))
        return respjson
       
    
    def order(self,catalog_entries,tag):
        session=self._build_session()
        token=self._get_auth_token(session)
        return self._send_order(session, token, catalog_entries, tag)
        

        
# '''
# Created on Oct 23, 2019
# 
# @author: banyait
# 
# This is a little testing snippet of ordering products on CreoDIAS
# '''
# 
# 
# def perform_search_get_first_n(session,numresults=1):
#     self.logger.debug("*** USING THE FINDER ***")
#     url='https://finder.creodias.eu/resto/api/collections/Sentinel1/search.json'
#     params={
#         'maxRecords':'10',
#         'productType':'RAW',
#         'sortParam':'startDate',
#         'sortOrder':'descending',
#         'status':'31|32',
#         'q':'mol',
#         'dataset':'ESA-DATASET'
#     }
# #     url="https://finder.creodias.eu/resto/api/collections/Sentinel2/search.json"
# #     params={
# #         'maxRecords':'10',
# #         'productIdentifier':'%T31UFS%',
# #         'startDate':'1970-01-01T00:00:00Z',
# #         'completionDate':'2070-01-01T23:59:59Z',
# #         'cloudCover':'[0,85]',
# #         'processingLevel':'LEVEL2A',
# #         'sortParam':'startDate',
# #         'sortOrder':'descending',
# #         'status':'31|32',
# #         'dataset':'ESA-DATASET'
# #     }
#     request=requests.Request(
#         "GET",
#         url=url,
#         params=params
#     )
#     respjson= _handle_request(session, request).json()
#     self.logger.debug(respjson['properties']['totalResults'])
#     # careful, if by default creodias paginates with 10
#     items=respjson['features'][:numresults]
#     for i in items:
#         del i['geometry']['coordinates']
#         del i['properties']['keywords']
#     self.logger.debug(json.dumps(items,indent=2))
#     return items
# 
# 
# def list_orders(session,token):
#     self.logger.debug("*** LIST ORDERS ***")
#     request=requests.Request(
#         "GET",
#         url="https://finder.creodias.eu/api/order/",
#         headers={
#             'Content-Type':'application/json',
#             'Keycloak-Token':token 
#         }
#     )
#     respjson= _handle_request(session, request).json()
#     self.logger.debug(json.dumps(respjson,indent=2))
#     return respjson
    

    
    
    
