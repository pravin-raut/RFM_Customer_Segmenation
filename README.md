# Customer Segmentation(RFM) using Azure Databricks

## Project Description

This project aims to perform customer segmentation using Azure Databricks. Customer segmentation is the process of dividing customers into groups based on recent purchase , how many time purchased and amount spend. The purpose of this is to better understand customer behavior, preferences, and needs, which can then be used to tailor marketing strategies, improve customer satisfaction, and ultimately increase revenue.

## Getting Started

To get started with this project, follow the steps below:

1. Create an Azure Databricks workspace and start a new cluster. You can choose the lowest configuration cluster to start with, which is the Standard_DS3_v2 instance with 2 cores and 14 GB RAM.
2. Create an Azure Data Lake Storage Gen2 (ADLS Gen2) account in the Azure portal.
3. In the ADLS Gen2 account, create a container for storing the IPL data.
4. In the Azure portal, create an Azure Key Vault.
5. Add a secret to the Key Vault that contains the ADLS Gen2 storage account key. This allows the Databricks cluster to access the ADLS Gen2 container.


## Data Flow
![Streaminh](https://user-images.githubusercontent.com/65663124/236552871-4da9e836-f551-4ae2-b509-138efb2bea4a.png)




## Usage

The customer segmentation project in Azure Databricks involves several key technologies and techniques to process books, customer, and orders data. 

Firstly, Spark Streaming is used for real-time data processing from various sources such as Kafka. This allows for ingesting and processing large volumes of data in real-time.

Secondly, Change Data Capture (CDC) is used on the Customers table to capture any changes made to it. This allows for efficient tracking of changes made to the data and processing only the changed data instead of the entire dataset.

Thirdly, Slowly Changing Dimension Type 2 (SCD-2) is used for the Books table. SCD-2 is a technique for handling changes to the dimensions in a data warehouse. It ensures that the history of changes to each dimension is preserved over time, which is important for accurate and meaningful analysis.

Fourthly, Merge operations are used to combine data from different sources. For example, merging data from the Customers table and the Orders table allows us to obtain information on customer behavior and purchasing habits.

Finally, K-Means is used for customer segmentation. K-Means is a clustering algorithm that groups similar customers together based on their purchasing behavior. This allows for targeted marketing campaigns and personalized recommendations to improve customer engagement and retention. 

In summary, the combination of Spark Streaming, CDC, SCD-2, Merge operations, and K-Means clustering provides a comprehensive and efficient pipeline for processing data and performing customer segmentation in Azure Databricks.

## Teaser

https://user-images.githubusercontent.com/65663124/236565606-f6ea642a-1d4d-4cf5-b853-942ed54cbd83.mp4


## Sample Output

![Cluster](https://user-images.githubusercontent.com/65663124/236554891-e88ac612-a009-46f4-9979-d77ee9a5cbd3.png)

![RFM](https://user-images.githubusercontent.com/65663124/236554904-b8cc5069-acef-489b-9c5e-7b2a4431afaa.png)

## Contact Information
https://www.linkedin.com/in/pravinraut16/

