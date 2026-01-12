# Data Analyst Agent using Hundreds of Datasets on Amazon Athena

## Table of Contents

- [Data Analyst Agent using Hundreds of Datasets on Amazon Athena](#data-analyst-agent-using-hundreds-of-datasets-on-amazon-athena)
  - [Table of Contents](#table-of-contents)
  - [Overview](#overview)
    - [Cost](#cost)
    - [Sample Cost Table](#sample-cost-table)
  - [Prerequisites](#prerequisites)
    - [Operating System](#operating-system)
    - [aws cdk bootstrap](#aws-cdk-bootstrap)
  - [Deployment Steps](#deployment-steps)
  - [Deployment Validation](#deployment-validation)
  - [Running the Guidance](#running-the-guidance)
  - [Next Steps](#next-steps)
  - [Cleanup](#cleanup)
  - [Notices](#notices)
  - [Authors](#authors)

## Overview
Organizations often manage hundreds of datasets across their data lakes, making it difficult for analysts to discover which datasets contain the information they need. Traditional keyword-based search falls short when users don't know the exact terminology or structure of available data. This creates a bottleneck where valuable data remains underutilized simply because it's hard to find.

This guidance provides a scalable approach for deploying a Data Analyst Agent that can query hundreds of datasets hosted on **Amazon Athena**. Built on the **Strands Agents** framework and deployed on **AWS AgentCore**, the agent leverages semantic search powered by **Amazon S3 Vectors** to automatically identify and retrieve the most relevant datasets based on user queries.

For each new dataset added to the system, the admin must upload two files:
1. A **Parquet file** with the raw data, which initialises the corresponding Athena table.
2. A **JSON metadata file** with a dataset description, which creates a vector database entry enabling semantic discovery by the agent.

To showcase the solution's ability to handle hundreds of datasets, this guidance includes a ready-to-use script that downloads all `337` publicly available datasets from the UK Office for National Statistics (ONS) and generates the corresponding Parquet data and JSON metadata files, ready to be uploaded. Additionally, a demo Streamlit Web-Application is provided, allowing users to interact with and query the agent through an intuitive interface.

![Reference Architecture Diagram](./data/media/architecture_diagram.drawio.png)

### Cost

This section is for a high-level cost estimate. Think of a likely straightforward scenario with reasonable assumptions based on the problem the Guidance is trying to solve. Provide an in-depth cost breakdown table in this section below ( you should use AWS Pricing Calculator to generate cost breakdown ).

Start this section with the following boilerplate text:

_You are responsible for the cost of the AWS services used while running this Guidance. As of <month> <year>, the cost for running this Guidance with the default settings in the <Default AWS Region (Most likely will be US East (N. Virginia)) > is approximately $<n.nn> per month for processing ( <nnnnn> records )._

Replace this amount with the approximate cost for running your Guidance in the default Region. This estimate should be per month and for processing/serving resonable number of requests/entities.

Suggest you keep this boilerplate text:
_We recommend creating a [Budget](https://docs.aws.amazon.com/cost-management/latest/userguide/budgets-managing-costs.html) through [AWS Cost Explorer](https://aws.amazon.com/aws-cost-management/aws-cost-explorer/) to help manage costs. Prices are subject to change. For full details, refer to the pricing webpage for each AWS service used in this Guidance._

### Sample Cost Table

**Note : Once you have created a sample cost table using AWS Pricing Calculator, copy the cost breakdown to below table and upload a PDF of the cost estimation on BuilderSpace. Do not add the link to the pricing calculator in the ReadMe.**

The following table provides a sample cost breakdown for deploying this Guidance with the default parameters in the US East (N. Virginia) Region for one month.

| AWS service  | Dimensions | Cost [USD] |
| ----------- | ------------ | ------------ |
| Amazon API Gateway | 1,000,000 REST API calls per month  | $ 3.50month |
| Amazon Cognito | 1,000 active users per month without advanced security feature | $ 0.00 |

## Prerequisites

### Operating System

- Talk about the base Operating System (OS) and environment that can be used to run or deploy this Guidance, such as *Mac, Linux, or Windows*. Include all installable packages or modules required for the deployment. 
- By default, assume Amazon Linux 2/Amazon Linux 2023 AMI as the base environment. All packages that are not available by default in AMI must be listed out.  Include the specific version number of the package or module.

**Example:**
“These deployment instructions are optimized to best work on **<Amazon Linux 2 AMI>**.  Deployment in another OS may require additional steps.”

- Include install commands for packages, if applicable.

### aws cdk bootstrap

<If using aws-cdk, include steps for account bootstrap for new cdk users.>

**Example blurb:** “This Guidance uses aws-cdk. If you are using aws-cdk for first time, please perform the below bootstrapping....”

## Deployment Steps
1. Install packages in requirements using command ```pip install requirement.txt```
2. Run this command to deploy the stack ```cdk deploy``` 
3. Download the `337` ONS datasets: ```python aws_data_analyst/download_datasets.py```
4. Preprocess the datasets: ```python aws_data_analyst/preprocess_datasets.py```
5. Upload the datasets to S3: ```python aws_data_analyst/upload_datasets_to_s3.py```

## Deployment Validation

<Provide steps to validate a successful deployment, such as terminal output, verifying that the resource is created, status of the CloudFormation template, etc.>


**Examples:**

* Open CloudFormation console and verify the status of the template with the name starting with xxxxxx.
* If deployment is successful, you should see an active database instance with the name starting with <xxxxx> in        the RDS console.
*  Run the following CLI command to validate the deployment: ```aws cloudformation describe xxxxxxxxxxxxx```

## Running the Guidance

<Provide instructions to run the Guidance with the sample data or input provided, and interpret the output received.> 

This section should include:

* Guidance inputs
* Commands to run
* Expected output (provide screenshot if possible)
* Output description

## Next Steps
The system can work with any other dataset, simply upload its parquet data file, and the JSON metadata file to the correspondent S3 buckets.

## Cleanup
To delete the CDK stacks:
```
cdk destroy --all
```

## Notices
*Customers are responsible for making their own independent assessment of the information in this Guidance. This Guidance: (a) is for informational purposes only, (b) represents AWS current product offerings and practices, which are subject to change without notice, and (c) does not create any commitments or assurances from AWS and its affiliates, suppliers or licensors. AWS products or services are provided “as is” without warranties, representations, or conditions of any kind, whether express or implied. AWS responsibilities and liabilities to its customers are controlled by AWS agreements, and this Guidance is not part of, nor does it modify, any agreement between AWS and its customers.*

## Authors
* Emilio Monti
* Ozan Cihangir
* Luis Orus
