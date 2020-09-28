# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def retrieveUidInfo(spark, to, os):
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

def retrieveUidIDFA(spark, to):
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

def retrieveAppInfo(spark):
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

def retrieveLBSRecords(spark, fr, to, os):
	table = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	uid_col = 'uid' if os == 'a' else 'md5(cast(uid as string))'
	sql = """
		select
			{0} uid,
			coordinate_source type
		from
			{1}
		where
			data_date between '{2}' and '{3}'
			and from_unixtime(itime, 'yyyyMMdd') between '{2}' and '{3}'
	""".format(uid_col, table, fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

def transform_to_row(row_dict):
	global args
	row_dict['data_date'] = args.query_month
	return Row(**row_dict)

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_Stats_Report___Cal_LBS_Stats') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--query_month', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	fr = args.query_month+'01'
	to = args.query_month+str(monthrange(int(args.query_month[:4]), int(args.query_month[4:]))[1])

	print('====> Start calculation')
	types = ['GPS', 'WIFI', 'CELL', 'IP']
	result = {}

	uids = retrieveUidInfo(spark, to, args.os)
	if args.os == 'i':
		uid_idfa = retrieveUidIDFA(spark, to)
		apps = retrieveAppInfo(spark)
		uid_idfa = uid_idfa.join(apps, on=['app_key'], how='inner').drop('app_key').select(['uid', 'app_package', 'device_id'])
		uids = uids.union(uids)
	else:
		devices = spark.read.csv('hgy/rlab_stats_report/sampled_devices/{0}'.format(args.query_month), header=True)\
					.select(F.col('imei').alias('device_id'))
		uids = uids.join(devices, on=['device_id'], how='inner')

	records = retrieveLBSRecords(spark, fr, to, args.os)
	records = records.join(uids, on=['uid'], how='inner')
	records = records.withColumn('type', F.when(records.type.contains('WIFI'), 'WIFI').when(records.type.contains('CELL'), 'CELL').otherwise(records.type)).cache()
	
	result['total_point_count'] = records.count()
	for t in types:
		result['{0}_point_count'.format(t)] = records.where(records.type == t).count()

	devices = records.repartition(20000, ['device_id']).groupBy(['device_id', 'type']).agg(\
		F.count(F.lit(1)).alias('device_lbs_times'), \
		F.countDistinct('app_package').alias('device_lbs_app_count'))
	devices = devices.withColumn('avg_app_times', F.col('device_lbs_times')/F.col('device_lbs_app_count')).cache()
	for t in types:
		devices_stats = devices.where(devices.type == t).agg(\
			F.count(F.lit(1)).alias('{0}_device_count'.format(t)), \
			F.mean('device_lbs_times').alias('avg_{0}_point_per_device'.format(t)), \
			F.mean('device_lbs_app_count').alias('avg_{0}_app_per_device'.format(t)), \
			F.mean('avg_app_times').alias('avg_{0}_point_per_app_per_device'.format(t))).collect()
		result['{0}_device_count'.format(t)] = devices_stats[0]['{0}_device_count'.format(t)]
		result['avg_{0}_point_per_device'.format(t)] = devices_stats[0]['avg_{0}_point_per_device'.format(t)]
		result['avg_{0}_app_per_device'.format(t)] = devices_stats[0]['avg_{0}_app_per_device'.format(t)]
		result['avg_{0}_point_per_app_per_device'.format(t)] = devices_stats[0]['avg_{0}_point_per_app_per_device'.format(t)]
	
	result = sc.parallelize([result]).map(transform_to_row).toDF()
	result.repartition(1).write.csv('/user/ronghui_safe/hgy/stats_report/lbs/{0}/{1}'.format(args.os, args.query_month, args.os), header=True)