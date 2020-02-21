#!/usr/bin/env python
# -*- coding: utf-8 -*-
import requests

class ObservationPlanUpload:
    
    uploadUrl = ""
    opSn=""
    opTime=""
    opType=""
    groupId=""
    unitId=""
    obsType=""
    gridId=""
    fieldId=""
    objId=""
    ra=""
    dec=""
    epoch=""
    objRa=""
    objDec=""
    objEpoch=""
    objError=""
    imgType=""
    expusoreDuring=""
    delay=""
    frameCount=""
    priority=""
    beginTime=""
    endTime=""
    pairId=""
 
    def __init__(self, uploadUrl, opSn,opTime,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,beginTime,endTime,pairId):
        self.uploadUrl = uploadUrl
        self.opSn=opSn
        self.opTime=opTime
        self.opType=opType
        self.groupId=groupId
        self.unitId=unitId
        self.obsType=obsType
        self.gridId=gridId
        self.fieldId=fieldId
        self.objId=objId
        self.ra=ra
        self.dec=dec
        self.epoch=epoch
        self.objRa=objRa
        self.objDec=objDec
        self.objEpoch=objEpoch
        self.objError=objError
        self.imgType=imgType
        self.expusoreDuring=expusoreDuring
        self.delay=delay
        self.frameCount=frameCount
        self.priority=priority
        self.beginTime=beginTime
        self.endTime=endTime
        self.pairId=pairId
        
        self.s = requests.Session()
    
    def sendPlan(self):
        parameters = {'opSn':self.opSn,
        'opTime':self.opTime,
        'opType':self.opType,
        'groupId':self.groupId,
        'unitId':self.unitId,
        'obsType':self.obsType,
        'gridId':self.gridId,
        'fieldId':self.fieldId,
        'objId':self.objId,
        'ra':self.ra,
        'dec':self.dec,
        'epoch':self.epoch,
        'objRa':self.objRa,
        'objDec':self.objDec,
        'objEpoch':self.objEpoch,
        'objError':self.objError,
        'imgType':self.imgType,
        'expusoreDuring':self.expusoreDuring,
        'delay':self.delay,
        'frameCount':self.frameCount,
        'priority':self.priority,
        'beginTime':self.beginTime,
        'endTime':self.endTime,
        'pairId':self.pairId}
        r = self.s.post(self.uploadUrl, data=parameters, timeout=3, verify=False)
        #print r.encoding
        #resp = r.json()
        print(r.text)
            
# if __name__ == "__main__":
    
#     uploadUrl = "http://172.28.8.8/gwebend/observationPlanUpload.action"
#     opSn="123"
#     opTime="2017-11-27 11:57:35"
#     opType=""
#     groupId="001"
#     unitId="002"
#     obsType="ToO_manual"
#     gridId="G0008"
#     fieldId="01370245"
#     objId=""
#     ra="23.0"
#     dec="45.0"
#     epoch="2000"
#     objRa=""
#     objDec=""
#     objEpoch=""
#     objError=""
#     imgType="light"
#     expusoreDuring="10"
#     delay="5"
#     frameCount="345"
#     priority=""
#     beginTime=""
#     endTime=""
#     pairId=""
   
#     tplan = ObservationPlanUpload(uploadUrl, opSn,opTime,opType,groupId,unitId,obsType,gridId,fieldId,objId,ra,dec,epoch,objRa,objDec,objEpoch,objError,imgType,expusoreDuring,delay,frameCount,priority,beginTime,endTime,pairId)
#     tplan.sendPlan()
    