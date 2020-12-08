import ujson
import json

from dclibs import queuer, logs, rabbitmq_utils, config, rediscache, sfapi, postgres, utils, aws
LOGGER = logs.LOGGER

SERVICE_NAME='accountscreation'
SERVICE_DESCRIPTION="Generates accounts and insert them into Salesforce."
SERVICE_LABEL="Accounts Creation"
SERVICE_ATTRIBUTES=[
        {'AttributeName':'NamePattern', 'AttributeLabel': 'Name Pattern', 'AttributeDescription':'Name used to generated the accounts. Please enter a String.'},
        {'AttributeName':'AccountQuantity', 'AttributeLabel': 'Record Quantity', 'AttributeDescription':'Number of records to generate. Please enter a positive number'}
    ]
SERVICE_STRUCTURE = {'ServiceName':SERVICE_NAME,
    'ServiceLabel':SERVICE_LABEL,
    'ServiceDescription':SERVICE_DESCRIPTION,
    'ServiceAttributes' : SERVICE_ATTRIBUTES,
    'PublishExternally':True}
###


def generateAccount(rows, namePattern, ownerId):
    CSV_Structure = 'Name,OwnerId'
    fileName = '/tmp/' + namePattern + '.csv' 
    f = open(fileName ,'w')
    f.write("Name,OwnerId\n")
    for i in range (0, rows, 1):
        unique_name=namePattern + ' - ' + i.__str__() + ','  + ownerId
        f.write(unique_name  + "\n")
    f.close()
    return fileName, CSV_Structure

def treatMessage(dictValue):
    LOGGER.info(dictValue)
    # notifies the user the service starts treating
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Process Started', True)
    # make sure we have the proper params
    LOGGER.info(dictValue['data']['payload']['ComputeAttributes__c'])
    computeAttributes = dictValue['data']['payload']['ComputeAttributes__c'].replace("'","\"")
    #LOGGER.info(ujson.loads(computeAttributes))
    computeAttributesDict = json.loads(computeAttributes)
    namePattern = computeAttributesDict['NamePattern']
    rows = int(computeAttributesDict['AccountQuantity'])
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Data Generation Started ', False)
    filename, CSV_Structure = generateAccount(rows, namePattern, dictValue['data']['payload']['CreatedById'])
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Data Generation Ended', False)
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Saving Data Generated Started', False)
    
    s3_url = aws.uploadData(filename, filename.split('/')[-1])
    dictValue['S3Url'] = s3_url
    dictValue['CSVStructure'] = CSV_Structure
    dictValue['SFObjectName'] = 'Account'
    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Saving Data Generated Done', False)
    # loading 
    # sends now to the bulk service
    queuer.sendToQueuer(dictValue, config.SERVICE_BULK)

    utils.serviceTracesAndNotifies(dictValue, SERVICE_NAME, SERVICE_NAME + ' - Process Ended', True)
    
# create a function which is called on incoming messages
def genericCallback(ch, method, properties, body):
    try:
        # transforms body into dict
        treatMessage(ujson.loads(body))
        
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        LOGGER.error(e.__str__())
def announce():
    # sends a message to the proper rabbit mq queue to announce himself
    queuer.sendToQueuer(SERVICE_STRUCTURE, config.SERVICE_REGISTRATION)    


if __name__ == "__main__":
    queuer.initQueuer()
    announce()
    queuer.listenToTopic(config.SUBSCRIBE_CHANNEL, 
    {
        config.QUEUING_KAFKA : treatMessage,
        config.QUEUING_CLOUDAMQP : genericCallback,
    })
