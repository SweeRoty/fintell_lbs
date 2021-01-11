# -*- coding:utf-8 -*-

from __future__ import division
import argparse
import configparser

from geolib import geohash
from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as F

def generateEdges(row):
	neighbors = geohash.neighbours(row['_c0'])
	results = []
	for neighbor in neighbors:
		results.append(Row(src=row['_c0'], _c0=neighbor))
	return results

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GraphFrame___Construct_Network') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--print_group_stats', action='store_true', default=False)
	args = parser.parse_args()

	nodes = spark.read.csv('/user/ronghui_safe/hgy/graphframe/all_geohash_20201031.txt', header=False)
	if args.print_group_stats:
		nodes = nodes.rdd.map(lambda row: Row(group=row['_c0'][:5], geohash=row['_c0'])).toDF()
		node_stats = nodes.groupby('group').agg(F.count('geohash').alias('hash_count'))
		quantiles = node_stats.approxQuantile('hash_count', [0.999, 0.99, 0.95, 0.9, 0.75, 0.5, 0.25, 0.01], 0.001)
		for i, percentile in enumerate([0.999, 0.99, 0.95, 0.9, 0.75, 0.5, 0.25, 0.01]):
			print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))
		print('---->')
		print(node_stats.describe('hash_count').show())
	edges = nodes.rdd.flatMap(generateEdges).toDF()
	edges = edges.join(nodes, on='_c0', how='inner')
	edges = edges.select(F.col('src'), F.col('_c0').alias('dst'))
	edges = edges.where(F.col('src') < F.col('dst'))
	edges.repartition(1).write.csv('/user/ronghui_safe/hgy/graphframe/all_geohash_20201031_edges.csv', header=True)