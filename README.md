# FP_BigData

**Kelompok 10**

| Nama                      | NRP        |
| ------------------------- | ---------- | 
| Johanes Edward Nathanael  | 5027231067 |
| Abhirama Triadyatma H     | 5027231061 |
| Rama Owarianto            | 5027231049 |

## Overview
Building an intelligent e-commerce analytics platform using modern data lakehouse architecture to create advanced product recommendation systems and business intelligence capabilities.

## Dataset

Source: [Amazon Products Dataset 2023](https://www.kaggle.com/datasets/asaniczka/amazon-products-dataset-2023-1-4m-products?select=amazon_products.csv).

Scale: 1.4 Million products

## Arsitektur

```mermaid
flowchart TD
    %% Data Sources & Ingestion Layer
    subgraph L1["[1] Sumber & Ingest Layer"]
        A1[Dataset CSV<br/>Amazon Products]
        A2[Real-time Events<br/>User Interactions]
        A3[Python Script<br/>Data Generator]
        A4[Apache Kafka<br/>Message Broker]
    end

    %% Processing & Data Lake Layer
    subgraph L2["[2] Processing & Data Lake Layer"]
        B1[Apache Spark<br/>ETL & ML Training]
        B2[Kafka Streams<br/>Real-time Processing]
        
        subgraph DL["Delta Lake on MinIO"]
            C1[Bronze Layer<br/>Raw Data]
            C2[Silver Layer<br/>Cleaned Data]
            C3[Gold Layer<br/>Aggregated Data]
        end
        
        B3[MLflow<br/>Model Registry]
    end

    %% Serving Layer
    subgraph L3["[3] Serving Layer"]
        D1[Trino<br/>Interactive Query]
        D2[Streamlit Dashboard<br/>Analytics & BI]
        D3[Flask API<br/>ML Model Serving]
    end

    %% User Layer
    subgraph L4["[4] Pengguna Layer"]
        E1[Data Analyst<br/>Business Intelligence]
        E2[Data Scientist<br/>Model Development]
        E3[Application<br/>Recommendation System]
    end

    %% Data Flow Connections
    A1 -->|Batch Ingestion| A4
    A2 -->|Stream Events| A4
    A3 -->|Generate Data| A4
    A4 -->|Process Streaming| B1
    A4 -->|Real-time Process| B2
    
    B1 -->|Store Raw| C1
    B1 -->|Transform| C2
    B1 -->|Aggregate| C3
    B2 -->|Stream Process| C2
    
    B1 -->|Train Models| B3
    B3 -->|Deploy Models| D3
    
    C1 -->|Query Bronze| D1
    C2 -->|Query Silver| D1
    C3 -->|Query Gold| D1
    
    D1 -->|Visualize| D2
    D1 -->|API Queries| D3
    C3 -->|Model Features| D3
    
    D2 -->|Analytics| E1
    D2 -->|Insights| E2
    D3 -->|Predictions| E3
    D3 -->|Model Metrics| E2

    %% Styling for different layers
    classDef sourceLayer fill:#ffebee,stroke:#d32f2f,stroke-width:2px
    classDef processLayer fill:#e8f5e8,stroke:#4caf50,stroke-width:2px
    classDef servingLayer fill:#e3f2fd,stroke:#2196f3,stroke-width:2px
    classDef userLayer fill:#fff3e0,stroke:#ff9800,stroke-width:2px
    classDef dataLake fill:#f3e5f5,stroke:#9c27b0,stroke-width:2px

    class A1,A2,A3,A4 sourceLayer
    class B1,B2,B3 processLayer
    class C1,C2,C3 dataLake
    class D1,D2,D3 servingLayer
    class E1,E2,E3 userLayer
```

## Tech Stack
