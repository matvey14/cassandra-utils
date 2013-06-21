cassandra-utils
===============

This is a collection of simple utilities that I've built for Cassandra.

cassandra_to_graphite.py
- This is a simple utility that queries Cassandra's MX4J web interface, captures all the metrics, and pipes them to Graphite. If you're looking to enable Graphite monitoring for your Cassandra cluster, this is probably the easiest way to get there.
- Prerequisites for use:
- Make sure you have MX4J installed and working.
