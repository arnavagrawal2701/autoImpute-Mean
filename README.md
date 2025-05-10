# Auto Impute with Mean on GCP
A pipeline to impute your dataset uploaded on Google BigQuery and Impute the dataset with mean values using DataProc Clusters

#### Install Requirements
Install the requirements using the following CLI command
pip install -r requirements.txt

#### Upload your dataset to Google Big Query
1. Go to Google Big Query
2. Create a dataset
3. Create a table and upload CSV and select the auto detect schema option

#### Upload the python file to gs storage using the following command
gsutil cp "main.py" gs://[project-name]/scripts/main.py 

#### Create a cluster in dataproc
1. Navigate to dataproc
2. Select Create Cluster
3. Choose the settings as per your requirement for the cluster

#### Upload job on your cluster using the following CLI Command
gcloud dataproc jobs submit pyspark gs://[project-name]/scripts/main.py --cluster [cluster-name] --region [cluster-region]
