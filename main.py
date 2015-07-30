from json import loads
from pandas import DataFrame, read_csv
from psycopg2 import connect, extras
from random import random
from re import sub
from requests import request

conn_string = "<PUT CONN_STRING HERE>"
STATS_SCHEMA = "stats"

LOCAL_ENTITIES = "local_entities"
UPDATE_STATS = "stats.bulkUpdateStats"
MATCH_STATS = "stats.matchUpdateStats"
JOB_LOG = "job_log"

SUCCESS = 200
TAMR_URL = "<PUT TAMR_URL HERE>"

def bulkUpdate(csv_file, targetSourceId):
    params = {"sourceId": targetSourceId}
    headers = {'content-type': 'application/json'}
    jsonPutFilename = convertToPutJson(csv_file)
    oldLocalEntitiesSize = getSizeOfTable(LOCAL_ENTITIES)
    
    with open(jsonPutFilename, "r") as jsonPutFile:
        r = request("POST", "{}/<PUT bulkUpdate URL HERE>".format(TAMR_URL),
                    params = params, headers = headers,
                  data = jsonPutFile, auth = ("<USERNAME>", "<PASSWORD>"))
        print "{} status: {}".format(getShortFilename(csv_file), r.status_code)

    if r.status_code == SUCCESS:
        jobId = getJobId(r)
        filename = getShortFilename(csv_file)
        fileRowCount = getSizeOfCsv(csv_file)

        newLocalEntitiesSize = getSizeOfTable(LOCAL_ENTITIES)
        newRecordCount = newLocalEntitiesSize - oldLocalEntitiesSize
        updateRecordCount = fileRowCount - newRecordCount

        with connect(CONNECT_STRING) as conn:
            with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
                cur.execute("INSERT INTO {} VALUES ({}, \'{}\', {}, {}, {});".format(
                                UPDATE_STATS, jobId, filename, fileRowCount,
                                updateRecordCount, newRecordCount))
                
    return r

def matchUpdate(csv_file, targetSourceId, recordId):
    params = {"sourceId": targetSourceId, "idField": recordId}
    headers = {'content-type': 'application/json'}
    jsonFilename = convertToJson(csv_file)

    with open(jsonFilename, "r") as jsonFile:
        r = request("POST", "{}/<PUT matchUpdate URL HERE>".format(TAMR_URL),
                    params = params, headers = headers,
                  data = jsonFile, auth = ("<USERNAME>", "<PASSWORD>"))
        print "{} status: {}".format(getShortFilename(csv_file), r.status_code)
        
    if r.status_code == SUCCESS:
        jobId = getJobIdForMatchUpdate()
        filename = getShortFilename(csv_file)
        fileRowCount = getSizeOfCsv(csv_file)

        certainCount, distinctCount, uncertainCount = getMatchCounts(r)

        with connect(CONNECT_STRING) as conn:
            with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
                cur.execute("INSERT INTO {} VALUES ({}, \'{}\', {}, {}, {}, {});".format(
                                MATCH_STATS, jobId, filename, fileRowCount,
                                certainCount, distinctCount, uncertainCount))

    return r

def cleanColumns(df):
    return df.filter(regex="^^(?!Unnamed).*$")

def convertToJson(csv_file):
    df = cleanColumns(read_csv(csv_file))
    json_file = sub("csv|txt", "json", csv_file)
    df.to_json(json_file, orient="records")

    return json_file

def convertToPutJson(csv_file):
    df = cleanColumns(read_csv(csv_file))
    putColumns = ["method", "recordId", "body"]
    putDf = DataFrame(columns = putColumns)

    for recordId in df.index:
        print "Converting data for recordId {recordId}...".format(recordId = recordId)
        body = {}
        
        for col in df.columns:
            body[str(col).strip()] = [str(df[col][recordId]).strip()]
        
        putDfRow = DataFrame([["PUT", str(recordId), body]], columns = putColumns)
        putDf = putDf.append(putDfRow)
    
    json_file = sub("csv|txt", "json", csv_file)
    putDf.to_json(json_file, orient="records")

    with open(json_file, 'r') as target:
        putData = target.read()

    target = open(json_file, 'w')
    putData = putData.replace("},{", "}\n\n{")[1:-1]
    target.write(putData)
    target.close()

    print "Successfully created put data!"
    return json_file


def getJobId(response):
    return int(loads(response.text)["payload"]["id"])

def getJobIdForMatchUpdate():
    with connect(CONNECT_STRING) as conn:
        with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
            cur.execute("SELECT MAX(id) from {job_log}".format(job_log=JOB_LOG))
            return int(cur.fetchone()['max'])
        
def getShortFilename(filename):
    return filename.split("/")[-1]

def getSizeOfCsv(csv_file):
    return len(read_csv(csv_file))

def getSizeOfTable(tableName):
    with connect(CONNECT_STRING) as conn:
        with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
            cur.execute("SELECT COUNT(*) FROM {tableName}".format(tableName=tableName))
            return int(cur.fetchone()['count'])

def getMatchCounts(response):
    payload = loads(response.text)["payload"]
    return len(payload["matches"]), len(payload["distinct"]), len(payload["uncertain"])

def setupSchema():
    with connect(CONNECT_STRING) as conn:
        with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
            cur.execute("DROP SCHEMA IF EXISTS stats;")
            cur.execute("CREATE SCHEMA stats;")
            
def setupStatTables():
    with connect(CONNECT_STRING) as conn:
        with conn.cursor(cursor_factory = extras.RealDictCursor) as cur:
            cur.execute("DROP TABLE IF EXISTS {update_stats}"
                        .format(update_stats=UPDATE_STATS)) 
            cur.execute(
                "CREATE TABLE {update_stats}".format(update_stats=UPDATE_STATS) +
                "(JobID int," +
                "Filename varchar(256)," +
                "FileRowCount int," +
                "OverwriteCount int," +
                "NewRecordCount int);")

            cur.execute("DROP TABLE IF EXISTS {match_stats}"
                        .format(match_stats=MATCH_STATS))
            cur.execute(
                "CREATE TABLE {match_stats}".format(match_stats=MATCH_STATS) +
                "(JobID int," +
                "Filename varchar(256)," +
                "FileRowCount int," +
                "CertainCount int," +
                "DistinctCount int," +
                "UncertainCount int);")

if __name__ == "__main__":    
    filename = "<PUT filename HERE>"
    sampleFilename = "<PUT sampleFilename HERE>"

    targetSourceId = "<PUT targetSourceId HERE>"
    recordId = "rec_id"
    sampleSize = 100

    setupSchema()
    setupStatTables()

    print "Running bulk update..."
    df = cleanColumns(read_csv(filename))[:sampleSize]
    df.to_csv(sampleFilename)
    
    r = bulkUpdate(sampleFilename, targetSourceId)    
    if r.status_code == SUCCESS:
        print "Success!\n"

    else:
        raise Exception("Exception thrown while running bulk update:\n\n{}"
                        .format(r.text))

    print "Running match update..."
    df = cleanColumns(read_csv(filename))[:sampleSize]
    df.to_csv(sampleFilename)

    r = matchUpdate(sampleFilename, targetSourceId, recordId)
    if r.status_code == SUCCESS:
        print "Success!\n"

    else:
        raise Exception("Exception thrown while running match update:\n\n{}"
                .format(r.text))

    print "Done"
