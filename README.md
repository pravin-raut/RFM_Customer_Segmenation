# Customer Segmentation using Azure Databricks

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

Azure Databricks is used to process the IPL data. The data is read from Azure Blob Storage, and processed using Spark streaming DataFrames and delta live tables. Various analyses are performed on the data, including but not limited to:

- Top batsmen and bowlers of the tournament
- Team-wise and player-wise statistics
- Analysis of player performance in various scenarios

The results of the analysis are stored in delta tables, and are visualized using databricks visualization. Interactive dashboards are created to display the results of the analysis.

## Teaser

https://user-images.githubusercontent.com/65663124/235992291-1a7728bb-dfe7-4998-b64e-2f423fd8e417.mp4

## Sample Output
![IPL](https://user-images.githubusercontent.com/65663124/236008312-88d732c0-33fc-48f6-8b47-567b4c04af68.png)



## Contact Information
https://www.linkedin.com/in/pravinraut16/

