# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
from itertools import product
from operator import add
import argparse
import numpy as np
import random
import time

from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

def getLBSRecords(spark, fr, to, os, agent):
	tbl = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	col = None
	alias = None
	if agent == 'app':
		if os == 'a':
			col = 'uid'
		else:
			col = 'md5(cast(uid as string))'
		alias = 'uid'
	else:
		if os == 'a':
			col = 'imei'
		else:
			col = 'idfa'
		alias = 'device_id'
	sql = """
		select
			{0} {1},
			cast(itime as long) itime,
			geo_hash_encode(coordinate, 6) geo_code
		from
			{2}
		where
			data_date between '{3}' and '{4}'
			and from_unixtime(itime, 'yyyyMMdd') between '{3}' and '{4}'
			and (coordinate_source like '%GPS%' or coordinate_source like '%WIFI%')
			and cast(lng as float) between 116.084 and 116.714 --(116.208, 116.549), (115.42, 117.51)
			and cast(lat as float) between 39.679 and 40.199 --(39.758, 40.024), (39.44, 41.06)
	""".format(col, alias, tbl, fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def getInvalidDevices(spark, data_date, os):
	device = 'imei' if os == 'a' else 'idfa'
	sql = """
		select
			{0} device_id,
			shanzhai_flag flag
		from
			ronghui_mart.sz_device_list
		where
			data_date = '{1}'
	""".format(device, data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def filterUnnecessaryData(t):
	arr = list(t[1])
	if len(arr) == 1:
		return False
	geo_set = set()
	for _, geo_hash in arr:
		geo_set.add(geo_hash)
	if len(geo_set) == 1:
		return False
	return True

def generateRawEdges(t):
	results = []
	geo_dict = {}
	for itime, geo_hash in t[1]:
		idate = datetime.fromtimestamp(itime)
		phase = 2*np.pi*(idate.hour*60+idate.minute)/1440
		if geo_hash in geo_dict:
			geo_dict[geo_hash].append(phase)
		else:
			geo_dict[geo_hash] = [phase]
	sorted_keys = sorted(geo_dict.keys())
	for i in range(len(sorted_keys)-1):
		phases_i = geo_dict[sorted_keys[i]]
		for j in range(i+1, len(sorted_keys)):
			phases_j = geo_dict[sorted_keys[j]]
			w_ij = min([len(phases_i), len(phases_j)])
			results.append(('_rlab_'.join([sorted_keys[i], sorted_keys[j]]), w_ij))
	return results

def sparsify(t):
	global gamma, partition, seed

	pb = gamma*t[1]/partition
	rs = int(time.time()*100)%seed
	random.seed(rs+t[1])
	if random.random() <= pb:
		return True
	else:
		return False

def transform2row(t):
	fr, to = t[0].split('_rlab_')
	return Row(fr=fr, to=to, weight=t[1])

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GEO_Project___Extract_Edges') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	parser.add_argument('--agent', choices=['device', 'app'])
	parser.add_argument('--threshold', type=int, default=604800)
	parser.add_argument('--gamma', type=float, default=1e6)
	parser.add_argument('--seed', type=int, default=202010)
	args = parser.parse_args()
	month_end = args.fr[:6]+str(monthrange(int(args.fr[:4]), int(args.fr[4:6]))[1])
	gamma = args.gamma
	seed = args.seed
	prefix = '/user/ronghui_safe/hgy/lbs/'

	print('====> Start computation')
	records = getLBSRecords(spark, args.fr, args.to, args.os, args.agent)
	if args.agent == 'app':
		uids = spark.read.csv('{0}uid/uid_mapping_{1}_{2}_{3}_bj'.format(prefix, args.fr, args.to, args.os), header=True)
		records = records.join(uids, on=['uid'], how='inner')
	else:
		invalid_devices = getInvalidDevices(spark, month_end, args.os)
		records = records.join(invalid_devices, on=['device_id'], how='left_outer').where(F.isnull(F.col('flag'))).drop('flag')
		invalid_devices = spark.read.csv('{0}devices/invalid_devices_{1}_{2}_OS{3}_thres{4}'.format(prefix, args.fr[4:], args.to[4:], args.os, args.threshold), header=True)
		records = records.join(invalid_devices, on=['device_id'], how='left_outer').where(F.isnull(F.col('flag'))).drop('flag')

	key = 'app_package' if args.agent == 'app' else 'device_id'
	if args.agent == 'app':
		records = records.groupby(['geo_code', key]).agg(F.count('itime').alias('evidence'))
		edges = records.select(F.col(key), F.col('geo_code').alias('fr'), F.col('evidence').alias('fr_e')) \
						.join(records.select(F.col(key), F.col('geo_code').alias('to'), F.col('evidence').alias('to_e')), on=key, how='inner') \
						.where(F.col('fr') < F.col('to'))
		edges = edges.withColumn('e', F.least('fr_e', 'to_e')).drop('fr_e').drop('to_e').cache()
		partition = edges.select(F.sum('e').alias('partition')).rdd.map(lambda row: row['partition']).collect()[0]
		edges = edges.rdd.map(lambda row: ('_rlab_'.join([row['fr'], row['to']]), row['e'])).filter(sparsify).map(lambda t: (t[0], 1)).reduceByKey(add).map(transform_to_row).toDF()
		edges.repartition(1).write.csv('{0}geo6_edges_{1}_{2}_{3}_{4}_{5}_bj'.format(prefix, args.fr[4:], args.to[4:], args.agent, args.os, gamma), header=True)
	else:
		edges = records.rdd \
						.map(lambda row: (row['{0}'.format(key)], (row['itime'], row['geo_code']))) \
						.groupByKey(numPartitions=40000) \
						.filter(filterUnnecessaryData) \
						.flatMap(generateRawEdges) \
						.cache()
		partition = float(edges.map(lambda t: t[1]).reduce(add)) # softmax
		edges = edges.filter(sparsify).map(lambda t: (t[0], 1)).reduceByKey(add).map(transform2row).toDF()
		edges.repartition(1).write.csv('{0}geo6_edges_{1}_{2}_{3}_{4}_{5}'.format(prefix, args.fr[4:], args.to[4:], args.agent, args.os, gamma), header=True)