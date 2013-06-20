import xml.etree.ElementTree as ET
import socket
import httplib
import urllib
import traceback
import getopt
import sys

from datetime import datetime
from string import maketrans

class CassandraToGraphite:
    
    graphiteHost = None
    graphitePort = 2003
    cassandraHost = '127.0.0.1'
    test = False
    
    SKIP_URLS = ['java.lang:type=MemoryPool,name=Code Cache', 'java.lang:type=MemoryPool,name=Par Eden Space', 'java.lang:type=MemoryPool,name=Par Survivor Space', 'org.apache.cassandra.db:type=ColumnFamilies,keyspace=OpsCenter', 'org.apache.cassandra.db:type=ColumnFamilies,keyspace=system', 'Server:name=XSLTProcessor', 'system:name=http']
    
    metrics = 0
        
    def sendMetric(self, name, value, time):
        metricsStr = name + " " + value + " " + time
        
        if (not self.test):
            sock = socket.socket()
            sock.connect((self.graphiteHost, self.graphitePort))
            sock.sendall(metricsStr)    
            sock.close()
        
        print metricsStr
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
        host = self.cassandraHost
        conn = httplib.HTTPConnection(host, 8081)
        conn.request("GET", url)
        response = conn.getresponse()
        if (response.status != 200):
            raise Exception("Received a non-200 response code: " + str(response.status))
        
        responseBody = response.read()
        return responseBody
        
    def sendAllMetrics(self, url):
        print "Graphite Host: " + self.graphiteHost
        print "Graphite Port: " + self.graphitePort
        print "Cassandra Host: " + self.cassandraHost
        
        
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

def usage():
    print "Usage: python cassandra_to_graphite.py -h <GRAPHITE_HOST> [-p <GRAPHITE_PORT>] [-c <CASSANDRA_HOST>]\n"
    print "Examples:"
    print "python cassandra_to_graphite.py -h 56.56.56.56 -p 2003 -c 127.0.0.1"
    print "python cassandra_to_graphite.py -h 56.56.56.56"

def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "h:p:c:t", ["host=", "port=", "cassandrahost=", "test"])
    except getopt.GetoptError as err:
        print str(err)
        usage()
        sys.exit(2)

    cassandraToGraphite = CassandraToGraphite()
    for o, a in opts:
        if o in ("-h", "--host"):
            cassandraToGraphite.graphiteHost = a
        elif o in ("-p", "--port"):
            cassandraToGraphite.graphitePort = a
        elif o in ("-c", "--cassandrahost"):
            cassandraToGraphite.cassandraHost = a
        elif o in ("-t", "--test"):
            cassandraToGraphite.test = True        
        else:
            assert False, "unhandled option"
    
    if (cassandraToGraphite.graphiteHost is None):
        usage()
        sys.exit(2)
    
    url = "/serverbydomain?template=identity"
    cassandraToGraphite.sendAllMetrics(url)

if __name__ == "__main__":
    main()

