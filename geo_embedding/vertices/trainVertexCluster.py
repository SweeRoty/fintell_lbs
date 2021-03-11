# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.ml.clustering import GaussianMixture
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.linalg import Vectors
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def getAbnormalAndroids(spark, query_date):
	sql = """
		select
			imei,
			shanzhai_flag flag
		from
			ronghui_mart.sz_device_list
		where
			data_date = '{0}'
	""".format(query_date)
	print(sql)
	devices = spark.sql(sql)
	return devices

def retrieveLBSRecords(spark, fr, to, os):
	table = 'ronghui_mart.rh_lbs_wifi_daily' if os == 'a' else 'ronghui_mart.user_location_log_daily_ios'
	sql = """
		select
			imei,
			cast(lng as float) lng,
			cast(lat as float) lat,
			city
		from
			{0}
		where
			data_date between '{1}' and '{2}'
			and from_unixtime(itime, 'yyyyMMdd') between '{1}' and '{2}'
			and coordinate_source like '%GPS%'
			and cast(lng as float) between 115.42 and 117.51
			and cast(lat as float) between 39.44 and 41.06
			--and city like '%北京%'
	""".format(table, fr, to)
	print(sql)
	records = spark.sql(sql)
	return records

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GEO_Project___Train_GMM_Model') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--fr', type=str)
	parser.add_argument('--to', type=str)
	parser.add_argument('--os', choices=['a', 'i'])
	args = parser.parse_args()
	month_end = args.to[:6]+str(monthrange(int(args.to[:4]), int(args.to[4:6]))[1])

	print('====> Start calculation')
	abnormal_imeis = getAbnormalAndroids(spark, month_end)
	points = retrieveLBSRecords(spark, args.fr, args.to, args.os)
	points = points.join(abnormal_imeis, on=['imei'], how='left_outer')
	points = points.where(points.flag.isNull())
	point_stats = points \
					.select(F.avg('lng').alias('avg_lng'), F.avg('lat').alias('avg_lat')) \
					.rdd \
					.map(lambda row: (row['avg_lng'], row['avg_lat'])) \
					.collect()[0]
	points = points.withColumn('lng', points.lng-point_stats[0]) #116.4736612393844
	points = points.withColumn('lat', points.lat-point_stats[1]) #39.930152107332574
	assembler = VectorAssembler(inputCols=['lng', 'lat'],outputCol="features")
	points = assembler.transform(points)
	for k in[81]:
		gmm = GaussianMixture(featuresCol='features', predictionCol='prediction', k=k, probabilityCol='probability', tol=0.01, maxIter=200).setSeed(11267)
		model = gmm.fit(points)
		model.save('/user/ronghui_safe/hgy/lbs/gmm_model/bj_{0}_{1}_{2}_{3}'.format(args.fr, args.to, args.os, k))
		summary = model.summary
		print('OUTPUT cluster_size: {0}'.format(summary.clusterSizes))
		print('OUTPUT likelihood: {0}'.format(summary.logLikelihood))
		print('OUTPUT iteration: {0}'.format(summary.numIter))
