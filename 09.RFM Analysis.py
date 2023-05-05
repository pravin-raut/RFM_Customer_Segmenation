# Databricks notebook source
# MAGIC %sql
# MAGIC USe rfm_analysis;

# COMMAND ----------

RFM_SegmentationDF=spark.sql("select * from RFM_Segmentation")

# COMMAND ----------

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Convert PySpark DataFrame to Pandas DataFrame
RFM_SegmentationPD = RFM_SegmentationDF.toPandas()

RFM_SegmentationPD


# COMMAND ----------

from scipy import stats
RFM_SegmentationPD_Fix = pd.DataFrame()
RFM_SegmentationPD_Fix["RecenyDays"] = stats.boxcox(RFM_SegmentationPD['RecenyDays'])[0]
RFM_SegmentationPD_Fix["Frequencey"] = stats.boxcox(RFM_SegmentationPD['Frequencey'])[0]
RFM_SegmentationPD_Fix["Monetory"] = stats.boxcox(RFM_SegmentationPD['Monetory'])[0]
RFM_SegmentationPD_Fix.tail()

# COMMAND ----------

# Import library
from sklearn.preprocessing import StandardScaler
# Initialize the Object
scaler = StandardScaler()
# Fit and Transform The Data
scaler.fit(RFM_SegmentationPD_Fix)
RFM_SegmentationPD_Normal = scaler.transform(RFM_SegmentationPD_Fix)
# Assert that it has mean 0 and variance 1
print(RFM_SegmentationPD_Normal.mean(axis = 0).round(2)) # [0. -0. 0.]
print(RFM_SegmentationPD_Normal.std(axis = 0).round(2)) # [1. 1. 1.]

# COMMAND ----------

from sklearn.cluster import KMeans
sse = {}
for k in range(1, 11):
    kmeans = KMeans(n_clusters=k, random_state=42)
    kmeans.fit(RFM_SegmentationPD_Normal)
    sse[k] = kmeans.inertia_ # SSE to closest cluster centroid
plt.title('The Elbow Method')
plt.xlabel('k')
plt.ylabel('SSE')
sns.pointplot(x=list(sse.keys()), y=list(sse.values()))
plt.show()

# COMMAND ----------

!pip install yellowbrick

# COMMAND ----------

from yellowbrick.cluster import KElbowVisualizer
from sklearn.cluster import KMeans
model = KMeans()
visualizer = KElbowVisualizer(model, k=(1,12))
visualizer.fit(RFM_SegmentationPD_Normal)  
visualizer.show()   

# COMMAND ----------


model = KMeans(n_clusters=4, random_state=42)
model.fit(RFM_SegmentationPD_Normal)
model.labels_.shape
RFM_SegmentationPD["Cluster"] = model.labels_
RFM_SegmentationPD.groupby('Cluster').agg({
    'RecenyDays':'mean',
    'Frequencey':'mean',
    'Monetory':['mean', 'count']}).round(2)
f, ax = plt.subplots(figsize=(25, 5))
ax = sns.countplot(x="Cluster", data=RFM_SegmentationPD)
RFM_SegmentationPD.groupby(['Cluster']).count()

# COMMAND ----------

RFM_SegmentationPD

# COMMAND ----------

RFM_SegmentationPD["Cluster"] = model.labels_
RFM_SegmentationPD.groupby('Cluster').agg({
    'RecenyDays':'mean',
    'Frequencey':'mean',
    'Monetory':['mean', 'count']}).round(2)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Note Below Observation can change with above values
# MAGIC ####Cluster 0
# MAGIC This cluster has customers with relatively recent purchases, low frequency, and low monetary value. These customers may be new to the business or may have made a one-time purchase. Possible marketing steps to retain these customers and increase their value to the business include:
# MAGIC
# MAGIC - Offering personalized recommendations based on their purchase history
# MAGIC - Providing special discounts or promotions to encourage them to make repeat purchases
# MAGIC - Sending personalized emails or newsletters to keep them engaged and informed about new products or services
# MAGIC - Offering incentives for them to refer friends or family to the business
# MAGIC
# MAGIC ####Cluster 1
# MAGIC
# MAGIC This cluster has customers with high monetary value, high frequency, and relatively recent purchases. These customers are likely to be the most valuable to the business. Possible marketing steps to retain these customers and increase their loyalty include:
# MAGIC
# MAGIC - Offering exclusive discounts or promotions for loyal customers
# MAGIC - Providing VIP treatment, such as early access to new products or special events
# MAGIC - Offering personalized recommendations and tailored content based on their purchase history and preferences
# MAGIC - Sending personalized thank-you notes or rewards to show appreciation for their business
# MAGIC
# MAGIC ####Cluster 2 
# MAGIC This cluster has customers with relatively recent purchases, high frequency, and moderate monetary value. These customers may be regular buyers of lower-priced items or may be testing the waters with the business. Possible marketing steps to increase their value to the business include:
# MAGIC
# MAGIC - Offering incentives for them to increase their purchase frequency or try higher-priced items
# MAGIC - Providing personalized recommendations based on their purchase history and preferences
# MAGIC - Offering personalized promotions or discounts based on their buying patterns
# MAGIC - Sending personalized follow-up emails or surveys to gather feedback and improve their experience
# MAGIC
# MAGIC ####Cluster 3
# MAGIC This cluster has customers with high monetary value, high frequency, and longer time since their last purchase. These customers may be loyal but may need some encouragement to return. Possible marketing steps to re-engage these customers and increase their purchase frequency include:
# MAGIC
# MAGIC - Sending personalized win-back emails or promotions to encourage them to make a purchase
# MAGIC - Offering exclusive discounts or promotions for returning customers
# MAGIC - Providing personalized recommendations based on their past purchases and preferences
# MAGIC - Offering incentives for them to refer friends or family to the business.
