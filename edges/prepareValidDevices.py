# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from operator import add
import argparse

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
			and cast(lng as float) between 116.084 and 116.714
			and cast(lat as float) between 39.679 and 40.199
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

if __name__ == '__main__':
	print('----> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GEO_Project___Prepare_Valid_Devices') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('----> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	parser.add_argument('--print_quantile', action='store_true', default=False)
	parser.add_argument('--threshold', type=int, default=604800)
	parser.add_argument('--save_abnormal', action='store_true', default=False)
	args = parser.parse_args()
	month_end = args.fr[:6]+str(monthrange(int(args.fr[:4]), int(args.fr[4:6]))[1])
	prefix = '/user/ronghui_safe/hgy/lbs/'

	print('----> Start computation')
	records = getLBSRecords(spark, args.fr, args.to, args.os, 'device')
	invalid_devices = getInvalidDevices(spark, month_end, args.os)
	records = records.join(invalid_devices, on=['device_id'], how='left_outer').where(F.isnull(F.col('flag')))
	devices = records.rdd.map(lambda row: (row['device_id'], 1)).reduceByKey(add).map(lambda t: Row(device_id=t[0], point_count=t[1])).toDF()
	if args.print_quantile:
		device_stats = devices.approxQuantile('point_count', [0.96, 0.97, 0.98, 0.99, 1.0], 0.01)
		for i, percentile in enumerate([0.96, 0.97, 0.98, 0.99, 1.0]):
			print('----> Quantile for {} is {}'.format(percentile, device_stats[i]))
	if args.save_abnormal:
		devices = devices.where(F.col('point_count') >= args.threshold).drop('point_count').withColumn('flag', F.lit(1))
		devices.repartition(1).write.csv('{0}devices/invalid_devices_{1}_{2}_OS{3}_thres{4}'.format(prefix, args.fr[4:], args.to[4:], args.os, args.threshold), header=True)