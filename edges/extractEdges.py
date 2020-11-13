# -*- coding: utf-8 -*-

from __future__ import division
from calendar import monthrange
from ConfigParser import RawConfigParser
from datetime import datetime
from itertools import product
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse
import numpy as np
import random
import time

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

def getUids(spark, to, os):
	device = 'imei' if os == 'a' else 'idfa'
	sql = """
		select
			distinct uid,
			package_name app_package,
			{0} device_id
		from
			ronghui.register_user_log
		where
			data_date <= '{1}'
			and platform = '{2}'
	""".format(device, to, os)
	print(sql)
	uids = spark.sql(sql)
	return uids

def getUidIDFA(spark, to):
	sql = """
		select
			distinct uid,
			idfa device_id,
			app_key
		from
			ronghui.uid2idfa_fact
		where
			data_date <= '{0}'
	""".format(to)
	print(sql)
	uids = spark.sql(sql)
	return uids

def getAppInfo(spark):
	sql = """
		select
			package app_package,
			app_key
		from
			ronghui_mart.app_info
	"""
	print(sql)
	apps = spark.sql(sql)
	return apps

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
			p_ij = 0
			for pi, pj in product(phases_i, phases_j):
				#p_ij += np.log2(3-np.sqrt((np.sin(pi)-np.sin(pj))**2+(np.cos(pi)-np.cos(pj))**2)/2)
				p_ij += 1
			p_ij = p_ij/(len(phases_i)*len(phases_j))
			w_ij = min([len(phases_i), len(phases_j)])
			results.append(('_rlab_'.join([sorted_keys[i], sorted_keys[j]]), (w_ij, p_ij)))
	return results

def sparsify(t):
	global gamma, partition, seed

	prob = gamma*t[1][0]/partition*t[1][1]
	seed = int(time.time()*100)%seed
	random.seed(seed+t[1][0])
	if random.random() <= prob:
		return True
	else:
		return False

def transform_to_row(t):
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
	parser.add_argument('--gamma', type=float, default=1e6)
	parser.add_argument('--seed', type=int, default=202010)
	args = parser.parse_args()
	month_end = args.fr[:6]+str(monthrange(int(args.fr[:4]), int(args.fr[4:6]))[1])
	gamma = args.gamma
	seed = args.seed

	print('====> Start computation')
	records = getLBSRecords(spark, args.fr, args.to, args.os, args.agent)
	if args.agent == 'app':
		uids = getUids(spark, args.to, args.os)
		if args.os == 'i':
			uid_idfa = getUidIDFA(spark, to)
			apps = getAppInfo(spark)
			uid_idfa = uid_idfa.join(apps, on=['app_key'], how='inner').drop('app_key').select(['uid', 'app_package', 'device_id'])
			uids = uids.union(uids)
		records = records.join(uids, on=['uid'], how='inner')
	invalid_devices = getInvalidDevices(spark, month_end, args.os)
	records = records.join(invalid_devices, on=['device_id'], how='left_outer').where(F.isnull(F.col('flag')))

	key = 'app_package' if args.agent == 'app' else 'device_id'
	edges = records.rdd \
					.map(lambda row: (row['{0}'.format(key)], (row['itime'], row['geo_code']))) \
					.groupByKey(numPartitions=20000) \
					.filter(filterUnnecessaryData) \
					.flatMap(generateRawEdges) \
					.cache()
	partition = float(edges.map(lambda t: t[1][0]).reduce(lambda x, y: x+y)) # softmax
	edges = edges.filter(sparsify).map(lambda t: (t[0], 1)).reduceByKey(lambda x, y: x+y).map(transform_to_row).toDF()
	prefix = '/user/ronghui_safe/hgy/lbs/'
	edges.repartition(1).write.csv('{0}geo6_edges_{1}_{2}_{3}_{4}_{5}'.format(prefix, args.fr[4:], args.to[4:], args.agent, args.os, gamma), header=True)