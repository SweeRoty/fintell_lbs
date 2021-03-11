# -*- coding: utf-8 -*-

from calendar import monthrange
from ConfigParser import RawConfigParser
from pyspark import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import Row, SparkSession

import argparse

def convert_6_7(row):
	chars = [str(i) for i in range(10)]
	for i in range(ord('a'), ord('z')+1):
		char = chr(i)
		if char not in ['a', 'i', 'l', 'o']:
			chars.append(char)
	rows = []
	for suffix in chars:
		rows.append(Row(geohash_6=row['geohash'], geohash=row['geohash']+suffix, community=row['community']))
	return rows

def retrieveGEOFeatures(spark, data_date):
	sql = """
		select
			imei geohash,
			get_json_object(score, '$.0.sc02') income,
			get_json_object(score, '$.0.sc03') consumption,
			get_json_object(score, '$.0.sc04') house_price,
			get_json_object(score, '$.0.sc06') age,
			get_json_object(score, '$.0.sc07') degree,
			get_json_object(score, '$.0.sc08') credit,
			get_json_object(score, '$.0.sc10') shared_debt,
			get_json_object(score, '$.0.sc11') financial_need,
			get_json_object(score, '$.0.sc12') debt_need
		from
			ronghui_mart.t_model_scores_parquet_data_date
		where
			data_date = '{0}'
			and model = 'g2'
	""".format(data_date)
	print(sql)
	features = spark.sql(sql)
	return features

if __name__ == '__main__':
	print('====> Initializing Spark APP')
	localConf = RawConfigParser()
	localConf.optionxform = str
	localConf.read('../../config')
	sparkConf = SparkConf()
	for t in localConf.items('spark-config'):
		sparkConf.set(t[0], t[1])
	spark = SparkSession.builder \
			.appName('RLab_GEO_Project___Extract_Vertex_Features') \
			.config(conf=sparkConf) \
			.enableHiveSupport() \
			.getOrCreate()
	sc = spark.sparkContext

	print('====> Parsing local arguments')
	parser = argparse.ArgumentParser()
	parser.add_argument('--data_date', type=str)
	parser.add_argument('--filename', type=str)
	args = parser.parse_args()
	#month_end = args.fr+str(monthrange(int(args.fr[:4]), int(args.fr[4:6]))[1])

	print('====> Start calculation')
	vertices = spark.read.csv('/user/ronghui_safe/hgy/lbs/{0}'.format(args.filename), header=True)
	vertices = vertices.select(F.col('Vertex_ID').alias('geohash'), F.col('Community').alias('community'))
	vertices = vertices.rdd.flatMap(convert_6_7).toDF()
	vertices = vertices.withColumn('geohash', F.md5('geohash'))
	features = retrieveGEOFeatures(spark, args.data_date)
	vertices = vertices.join(features, on='geohash', how='inner').drop('geohash')
	vertices = vertices.groupBy(['geohash_6', 'community'])\
						.agg(F.avg('income').alias('income'),\
						F.avg('consumption').alias('consumption'), \
						F.avg('house_price').alias('house_price'), \
						F.avg('age').alias('age'), \
						F.avg('degree').alias('degree'), \
						F.avg('credit').alias('credit'), \
						F.avg('shared_debt').alias('shared_debt'), \
						F.avg('financial_need').alias('financial_need'), \
						F.avg('debt_need').alias('debt_need'))
	vertices.repartition(1).write.csv('/user/ronghui_safe/hgy/lbs/{0}_with_feature'.format(args.filename), header=True)
