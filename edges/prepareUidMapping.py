# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getLBSUids(spark, fr, to, os):
	col = 'uid' if os == 'a' else 'md5(cast(uid as string))'
	tbl = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	sql = """
		select
			distinct {0} uid
		from
			{1}
		where
			data_date between '{2}' and '{3}'
			and from_unixtime(itime, 'yyyyMMdd') between '{2}' and '{3}'
			and (coordinate_source like '%GPS%' or coordinate_source like '%WIFI%')
			and cast(split(coordinate, ',')[0] as float) between 116.084 and 116.714 --(121.122, 121.793), (116.208, 116.549), (115.42, 117.51)
			and cast(split(coordinate, ',')[1] as float) between 39.679 and 40.199 --(30.872, 31.410), (39.758, 40.024), (39.44, 41.06)
	""".format(col, tbl, fr, to)
	print(sql)
	uids = spark.sql(sql)
	return uids

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

def getInvalidDevices(spark, data_date, os):
	col = 'imei' if os == 'a' else 'idfa'
	tbl = 'ronghui_mart.sz_device_list' if os == 'a' else 'ronghui_mart.sz_device_list_ios'
	sql = """
		select
			distinct {0} device_id,
			shanzhai_flag flag
		from
			{1}
		where
			data_date = '{2}'
	""".format(col, tbl, data_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GEO_Project___Extract_UID_Mappings') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'], default='a')
	args = parser.parse_args()
	month_end = args.fr[:6]+str(monthrange(int(args.fr[:4]), int(args.fr[4:6]))[1])

	print('====> Start computation')
	uids_lbs = getLBSUids(spark, args.fr, args.to, args.os)

	uids = getUids(spark, args.to, args.os)
	if args.os == 'i':
		uids_idfa = getUidIDFA(spark, args.to)
		apps = getAppInfo(spark)
		uids_idfa = uids_idfa.join(apps, on=['app_key'], how='left_outer').select(uids.columns)
		uids = uids.union(uids_idfa)
	uids_lbs = uids_lbs.join(uids, on='uid', how='inner')
	invalid_devices = getInvalidDevices(spark, month_end, args.os)
	uids_lbs = uids_lbs.join(invalid_devices, on='device_id', how='left_outer')
	uids_lbs = uids_lbs.where(uids_lbs.flag.isNull()).drop('flag').drop('device_id')
	uids_lbs.repartition(1).write.csv('/user/ronghui_safe/hgy/lbs/uid/uid_mapping_{0}_{1}_{2}_bj'.format(args.fr, args.to, args.os), header=True)