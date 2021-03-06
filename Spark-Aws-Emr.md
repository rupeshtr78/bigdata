---
layout: post
title: AWS EMR Spark
tags: [spark,aws,emr,scala,jekyll]
image: '/images/awsemr/putty-emr-login.png'
---

**Steps for Deploying Spark App on Amazon EMR**

1. Create a spark application jar file , 
2. Upload jar files are related files to S3 bucket , 
3. Deploy AWS Emr Spark Yarn Cluster and 
4. Submit-spark job on the EMR yarn spark cluster.

- **Step 1** 

  Test your application using Scala - ide using sample data.

  ![](/images/awsemr/scala-ide01.png)

  

- **Step 2**

  - Remove all local path and Spark Context master local reference from Scala file.

  - Use SBT to package your application

    - Create and empty directory **sbt**
    - **sbt new scala/hello-world.g8.** 
    - add your scale files under sbt\movies\src\main\scala directory
    - edit sbt\movies\built.sbt

  - ```yaml
    name := "MostRatedMovies100k"
    
    version := "1.0"
    
    organization := "com.forsynet.sparkemr"
    
    scalaVersion := "2.11.12"
    
    libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.5" % "provided"
    )
    ```

    

  - at command prompt run command "sbt assembly"

  - This will create jar files with all dependencies under sbt\movies\target\scala-2.11\MostRatedMovies100k-assembly-1.0.jar

- **Step 3**

  - upload the jar files and the data files to s3 bucket .Use UI or below cli commands

  - ```powershell
    aws configure
    aws s3api create-bucket --bucket rupeshemr
    aws s3 sync data/
    
    ```

    - Verify the data is uploaded to s3 bucket
    - ![](/images/awsemr/s3-upload-files.png)



## Create an Amazon EMR cluster

- **Step 4**

- Use the aws cli or the UI to create the cluster.

- ```shell
  aws emr create-cluster \
      --instance-type m3.xlarge \
      --release-label emr-5.10.0 \
      --service-role EMR_DefaultRole \
      --ec2-attributes InstanceProfile=EMR_EC2_DefaultRole \
      --security-configuration mySecurityConfiguration \
      --kerberos-attributes file://kerberos_attributes.json
  ```

  - Verify Cluster Creation
  - ![](/images/awsemr/aws-emr01.png)

  - **Step5**

  - Add SSH Inbound rule to security groups
  - ![](/images/awsemr/aws-emr-securitygrp.png)

- **Step 6**
- ssh into emr master node
- ![](/images/awsemr/putty-emr-login.png)

- Copy your driver program’s JAR file and any other files required.

- ```
  aws s3 cp s3://rupeshemr/MostRatedMovies-1.0.jar ./
  ```

  ![](/images/awsemr/aws-s3-cp-jar.png)



## Submit the Spark Job

- **Step 7**

  - ```sh
    spark-submit MostRatedMovies-1.0.jar
    ```

- Verify Results of top rated movies
  
- ![](/images/awsemr/spark-submit-results.png)
  
- Use the Spark History Server UI to see the Spark Job History for submitted job
- ![](/images/awsemr/spark-historyui01.png)

![](/images/awsemr/spark-historyui01-step1.png)

- Verify Amazon s3 bucket for logs created for the job

![](/images/awsemr/s3-emr-output.png)