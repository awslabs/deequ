# Installing AWS Deequ in Azure Synapse

Follow these steps to install AWS Deequ in your Azure Synapse environment:

## 1. Download the Deequ JAR
Download the Deequ JAR that matches the Spark version of your Synapse Spark pool. As of September 2024, the required version is Deequ 2.0.7 for Spark 3.4.

- **Deequ JAR:** [Deequ 2.0.7-spark-3.4](https://mvnrepository.com/artifact/com.amazon.deequ/deequ/2.0.7-spark-3.4)

This uses mvrepository.com

## 2. Add JAR to workspace

![image](https://github.com/user-attachments/assets/7153f843-48b9-44e4-93a2-5e87d8855df7)

### Notes
Adding packages to a running Spark pool might take time as the cluster may need to be reprovisioned.

Azure Synapse allows storing JAR files in an Azure Storage Account, which can be linked to your Spark pool.

- **Instructions:** [Manage Workspace Packages in Azure Storage](https://learn.microsoft.com/en-us/azure/synapse-analytics/spark/apache-spark-manage-workspace-packages#storage-account)

## 3. Add JAR to Spark Pool
Once the JAR is downloaded locally:
   - Navigate to your Synapse Studio.
   - Go to the **Manage** tab and select **Apache Spark pools**.
   - Choose your Spark pool and add the uploaded JAR file under **Packages**.

![image](https://github.com/user-attachments/assets/784fd61a-33a3-44b2-8f4b-cb845c66e13f)


### Notes
Adding packages to a running Spark pool might take time as the cluster may need to be reprovisioned.

## 4. Access Deequ in Synapse Notebook
After the JAR is added to the Spark pool, you can use Deequ in any Azure Synapse Spark notebook:

![image](https://github.com/user-attachments/assets/d9fd7ce7-2e13-47ae-ad0b-c5f78bea0600)
