#renaming multiple columns in spark
df1 = spark.read.format("csv").option("header", "true").load(input_path)
df1 = df1.withColumnRenamed("Semiotic Class","semioticclass").withColumnRenamed("Input Token","inputtoken").withColumnRenamed("Output Token","outputtoken")
