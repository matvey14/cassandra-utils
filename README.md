cassandra-utils
===============

This is a collection of simple utilities that I've built for Cassandra.

cassandra_to_graphite.py
------------------------
This is a simple utility that queries Cassandra's MX4J web interface, captures all the metrics, and pipes them to Graphite. If you're looking to enable Graphite monitoring for your Cassandra cluster, this is probably the easiest way to get there.

Prerequisites:
- Make sure you have MX4J installed and working. Try hitting http://127.0.0.1:8081 where 127.0.0.1 is the IP of the Cassandra host. You can refer to the following for Instructions: http://wiki.apache.org/cassandra/Operations#Monitoring_with_MX4J

Usage: 
- python cassandra_to_graphite.py -h <GRAPHITE_HOST> [-p <GRAPHITE_PORT>] [-c <CASSANDRA_HOST>]

Examples:
- python cassandra_to_graphite.py -h 56.56.56.56 -p 2003 -c 127.0.0.1
- python cassandra_to_graphite.py -h 56.56.56.56

You can simply schedule this script to run as a cronjob on every C* node. Keep in mind that this script will produce little over 200 metrics, so make sure your Graphite installation is up to the challenge.
