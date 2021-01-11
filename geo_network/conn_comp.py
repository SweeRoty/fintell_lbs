# -*- coding:utf-8 -*-

from __future__ import division
import argparse
import configparser

from graphframes import *
from pyspark import SparkConf
from pyspark.sql import Row, SparkSession
import pyspark.sql.functions as F

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = configparser.ConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GraphFrame___Connected_Components') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--print_comp_stats', action='store_true', default=False)
	args = parser.parse_args()

	nodes = spark.read.csv('/user/ronghui_safe/hgy/graphframe/all_geohash_20201031.txt', header=False)
	nodes = nodes.select(F.col('_c0').alias('id'))
	edges = spark.read.csv('/user/ronghui_safe/hgy/graphframe/all_geohash_20201031_edges.csv', header=True)
	edges = edges.withColumn('relationship', F.lit('connects_with'))
	graph = GraphFrame(nodes, edges)
	sc.setCheckpointDir('/user/ronghui_safe/hgy/graphframe/conn_comp')
	comps = graph.connectedComponents()
	comps.repartition(1).write.csv('/user/ronghui_safe/hgy/graphframe/conn_comp/all_geohash_20201031_comps', header=True)
	if args.print_comp_stats:
		comp_stats = comps.groupby('component').agg(F.count('id').alias('comp_size'))
		quantiles = comp_stats.approxQuantile('comp_size', [0.99, 0.95, 0.9, 0.75, 0.5, 0.25, 0.1, 0.05, 0.01], 0.001)
		for i, percentile in enumerate([0.99, 0.95, 0.9, 0.75, 0.5, 0.25, 0.1, 0.05, 0.01]):
			print('----> Quantile for {} is {}'.format(percentile, quantiles[i]))
		print('---->')
		print(comp_stats.describe('comp_size').show())