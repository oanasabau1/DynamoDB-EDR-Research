# DynamoDB-EDR-Research

This team project aims to optimize DynamoDB table structures for handling large-scale, complex sensor data, specifically from vehicle EDRs and radar sensors. By simulating realistic data scenarios, we will develop efficient data models, management techniques, and query strategies to ensure optimal performance and scalability in DynamoDB.

## Key Features

### 1. Data Modeling Exploration
Conducted a comprehensive analysis of DynamoDB data modeling approaches, focusing on:
- **Entity-Attribute-Value (EAV) Models**
- **Hierarchical Structures**
- **Normalization/Denormalization**
- **Composite Key Strategies**

### 2. Optimized Schema Design
- Developed a DynamoDB table schema tailored for efficient handling of EDR and radar data.
- The schema is defined in an **AWS CloudFormation template** for seamless deployment and management.

### 3. Synthetic Data Generation
- Implemented functions to **create** and **delete synthetic data**, simulating real-world scenarios for testing and development purposes.
- Provides a flexible way to generate test data that mimics production environments.

### 4. Accident Data Analysis
- Developed a function to analyze radar and EDR events, extracting critical accident information such as:
  - **Vehicles**
  - **Pedestrians**
  - **Cyclists**
- The analysis is based on **high detection confidence** to ensure accuracy.

### 5. Indexing Strategy Optimization
- Evaluated and implemented various indexing strategies:
  - **Global Secondary Index (GSI)**
  - **Local Secondary Index (LSI)**
  - **Composite Keys**
- Optimized query performance using a mix of **scans** and **queries** based on access patterns.

  

I have included a schema of tables with the partition and sort keys used.

![Picture1](https://github.com/user-attachments/assets/abfb07cb-bba2-4b2a-8d84-451d1016029b)
