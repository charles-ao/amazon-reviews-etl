# Amazon Reviews NLP Analytics (AWS S3 | PySpark | Glue  | Athena)

## Overview
This project builds a low-cost, scalable NLP analytics pipeline to analyze Amazon customer reviews using the fast.ai NLP dataset.  
The pipeline ingests raw review data from a public Amazon S3, performs distributed ETL and NLP enrichment with AWS Glue (PySpark), stores curated datasets in S3 as Parquet, and enables serverless analytics using Amazon Athena.

The goal is to demonstrate:
- Cloud-based ETL design
- NLP feature extraction at scale
- Analytics-ready data lake modeling
- Serverless querying without a dedicated warehouse

---

## Dataset
**Source:** [fast.ai NLP datasets ](https://registry.opendata.aws/fast-ai-nlp/) 
**Location:** `s3://fast-ai-nlp/amazon_review_full_csv.tgz`

Each review record contains:
- Rating (1–5)
- title
- Review

The dataset includes millions of Amazon reviews and is commonly used for large-scale NLP tasks.

---

## Architecture

![alt text](architecture.png)

