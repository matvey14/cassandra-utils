import xml.etree.ElementTree as ET
import socket
import httplib
import urllib
import sys
import traceback

from datetime import datetime
from string import maketrans

class CassandraToGraphite:
    
    GRAPHITE_HOST = None
    GRAPHITE_PORT = None
    
    SKIP_URLS = ['java.lang:type=MemoryPool,name=Code Cache', 'java.lang:type=MemoryPool,name=Par Eden Space', 'java.lang:type=MemoryPool,name=Par Survivor Space', 'org.apache.cassandra.db:type=ColumnFamilies,keyspace=OpsCenter', 'org.apache.cassandra.db:type=ColumnFamilies,keyspace=system', 'Server:name=XSLTProcessor', 'system:name=http']
    
    test = False
    metrics = 0
    
    def __init__(self):
        self.initGraphite()

    def initGraphite(self):
        with open('/etc/graphitehost', 'r') as f:
            self.GRAPHITE_HOST = f.read().rstrip()
        
        with open('/etc/graphiteport', 'r') as f:
            self.GRAPHITE_PORT = int(f.read().rstrip())   
    
    GRAPHITE_HOST = None
    GRAPHITE_PORT = None
        
    def sendMetric(self, name, value, time):
        str = name + " " + value + " " + time
        
        if (not o.test):
            sock = socket.socket()
            sock.connect((self.GRAPHITE_HOST, self.GRAPHITE_PORT))
            sock.sendall(str)    
            sock.close()
        
        print str
        self.metrics += 1
    
    def sendMetricsFromXml(self, xmlStr):
        root = ET.fromstring(xmlStr.strip())
        
        objectname = root.attrib["objectname"]
        prefix = "cassandra." + getHost() + "." + objectname.translate(maketrans(":,", ".."))
        time = datetime.now().strftime('%s')
        
        for child in root:
            if child.tag == "Attribute":
                type = child.attrib["type"]
                if (type == "long" or type == "double" or type == "int"):
                    name = child.attrib["name"]
                    value = child.attrib["value"]
                    if (not isNumber(value)):
                        continue
                    
                    name = prefix + "." + name
                    self.sendMetric(name, value, time)
                    
    def sendMetricsFromUrl(self, url):
        responseBody = self.getPage(url)
        self.sendMetricsFromXml(responseBody)
        
    def getPage(self, url):
        host = "127.0.0.1"
        conn = httplib.HTTPConnection(host, 8081)
        conn.request("GET", url)
        response = conn.getresponse()
        if (response.status != 200):
            raise Exception("Received a non-200 response code: " + str(response.status))
        
        responseBody = response.read()
        return responseBody
        
    def sendAllMetrics(self, url):
        xmlStr = self.getPage(url)
        
        root = ET.fromstring(xmlStr.strip())
        for child in root:
            for mbean in child.findall("MBean"):
                objectname=mbean.attrib["objectname"]
                if self.skipObject(objectname):
                    continue
                
                objectname = urllib.quote(objectname)
                url = "/mbean?objectname=" + objectname + "&template=identity"
                try:
                    self.sendMetricsFromUrl(url)
                except:
                    traceback.print_exc(file=sys.stderr)
        
        print "Sent " + str(self.metrics) + " metrics to Graphite"

    def skipObject(self, objectname):
        for skipUrl in self.SKIP_URLS:
            if objectname.startswith(skipUrl):
                return True;
        return False
    
def getHost():
    hostname = socket.gethostname()
    return hostname.split(".", 1)[0]

def isNumber(s):
    if (s is not None and s == "NaN"):
        return False
    
    try:
        float(s)
        return True
    except ValueError:
        return False

o = CassandraToGraphite()
if len(sys.argv) > 1 and sys.argv[1] == "-test":
    o.test = True
    print "Running in test mode"
url = "/serverbydomain?template=identity"
o.sendAllMetrics(url)

