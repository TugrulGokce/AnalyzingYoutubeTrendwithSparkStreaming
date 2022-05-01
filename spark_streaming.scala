

// to start spark streaming
./spark-shell --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 ...

// Connecting Kafka 
val UsYoutubeDf = spark.readStream.format("kafka")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("subscribe", "usyoutube").load


// import StructType for Schema (StringType, IntegerType, BooleanType)
import org.apache.spark.sql.types._

// Defining Schema
val activationSchema = StructType(List( StructField("video_id", StringType, true),
      StructField("title", StringType, true),
      StructField("published_at", StringType, true), 
      StructField("channel_id", StringType, true),			
      StructField("channel_title", StringType, true),
      StructField("category_id", IntegerType, true),
      StructField("trending_date", StringType, true),
      StructField("view_count", IntegerType, true),
      StructField("likes", IntegerType, true),
      StructField("dislikes", IntegerType, true),
      StructField("comment_count", IntegerType, true),
      StructField("comments_disabled", BooleanType, true),
      StructField("ratings_disabled", BooleanType, true),
      StructField("category_title", StringType, true)))

// JSON to df
val youTubeSchemaDf =  UsYoutubeDf
		.select(from_json($"value".cast("string"), activationSchema)
		.alias("usYoutube"))
		.select("usYoutube.*")

youTubeSchemaDf.printSchema

/*
root
 |-- video_id: string (nullable = true)
 |-- title: string (nullable = true)
 |-- published_at: string (nullable = true)
 |-- channel_id: string (nullable = true)
 |-- channel_title: string (nullable = true)
 |-- category_id: integer (nullable = true)
 |-- trending_date: string (nullable = true)
 |-- view_count: integer (nullable = true)
 |-- likes: integer (nullable = true)
 |-- dislikes: integer (nullable = true)
 |-- comment_count: integer (nullable = true)
 |-- comments_disabled: boolean (nullable = true)
 |-- ratings_disabled: boolean (nullable = true)
 |-- category_title: string (nullable = true)
*/


//mostviewedchannel 
val mostviewedchannelDf = youTubeSchemaDf
		.groupBy("trending_date","channel_title")
		.agg(sum("view_count").as("total_view_count_by_channel"))
		.sort($"total_view_count_by_channel".desc)

import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val mostviewedchannelDfQuery = mostviewedchannelDf
		.selectExpr("CAST(trending_date as STRING)","CAST(channel_title AS STRING)", "CAST(total_view_count_by_channel AS INTEGER)", "to_json(struct(*)) AS value")
		.writeStream.format("kafka")
		.option("checkpointLocation", "/home/tugrulgkc34/consumermostviewedchannel")
		.option("failOnDataLoss", "false")
		.outputMode("complete")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("topic", "mostviewedchannel").start()


//mostviewedcategory 
val mostviewedcategoryDf = youTubeSchemaDf
		.groupBy("trending_date","category_title")
		.agg(sum("view_count").as("total_view_count_by_category"))
		.sort($"total_view_count_by_category".desc)

val mostviewedcategoryDfQuery = mostviewedcategoryDf
		.selectExpr("CAST(trending_date as STRING)","CAST(category_title AS STRING)", "CAST(total_view_count_by_category AS INTEGER)", "to_json(struct(*)) AS value")
		.writeStream.format("kafka")
		.option("checkpointLocation", "/home/tugrulgkc34/consumermostviewedcategory")
		.option("failOnDataLoss", "false")
		.outputMode("complete")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("topic", "mostviewedcategory").start()



//mostlikedchannel  
val mostlikedchannelDf = youTubeSchemaDf
		.groupBy("trending_date","channel_title")
		.agg(sum("likes").as("total_likes_count_by_channel"))
		.sort($"total_likes_count_by_channel".desc)

val mostlikedchannelDfDfQuery = mostlikedchannelDf
		.selectExpr("CAST(trending_date as STRING)","CAST(channel_title AS STRING)", "CAST(total_likes_count_by_channel AS INTEGER)", "to_json(struct(*)) AS value")
		.writeStream.format("kafka")
		.option("checkpointLocation", "/home/tugrulgkc34/consumermostlikedchannel")
		.option("failOnDataLoss", "false")
		.outputMode("complete")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("topic", "mostlikedchannel").start()


//mostlikedcategory 
val mostlikedcategoryDf = youTubeSchemaDf
		.groupBy("trending_date","category_title")
		.agg(sum("likes").as("total_likes_count_by_category"))
		.sort($"total_likes_count_by_category".desc)

val mostlikedcategoryDfQuery = mostlikedcategoryDf
		.selectExpr("CAST(trending_date as STRING)","CAST(category_title AS STRING)", "CAST(total_likes_count_by_category AS INTEGER)", "to_json(struct(*)) AS value")
		.writeStream.format("kafka")
		.option("checkpointLocation", "/home/tugrulgkc34/consumermostlikedcategory")
		.option("failOnDataLoss", "false")
		.outputMode("complete")
		.option("kafka.bootstrap.servers", "localhost:9092")
		.option("topic", "mostlikedcategory").start()

