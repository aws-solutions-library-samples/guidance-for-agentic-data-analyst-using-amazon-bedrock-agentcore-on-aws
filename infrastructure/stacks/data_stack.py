from aws_cdk import (
    Stack,
    RemovalPolicy,
    Duration,
    aws_s3 as s3,
    aws_glue as glue,
    aws_athena as athena,
    aws_lambda as lambda_,
    aws_iam as iam,
    aws_s3_notifications as s3n,
    aws_ssm as ssm
)
from constructs import Construct
from cdk_nag import NagSuppressions
from pathlib import Path

# See: https://github.com/bimnett/cdk-s3-vectors/blob/main/examples/python.py
import cdk_s3_vectors as s3_vectors


DEFAULT_EMBEDDER = "nova"
EMBEDDING_DIMENSION = 1024


def construct_vector_db(stack, postfix=""):
    id_postfix = postfix.title()
    name_postfix = f"-{postfix.lower()}" if postfix else ""

    # We use cdk-s3-vectors as high-level CDK construct https://constructs.dev/packages/cdk-s3-vectors/
    # When AWS CloudFormation introduces native support, this construct can be replaced.
    dataset_embeddings = s3_vectors.Bucket(
        stack, "DatasetEmbeddings" + id_postfix,
        vector_bucket_name="dataset-embeddings" + name_postfix,
    )

    dataset_embeddings_index = s3_vectors.Index(
        stack, "DatasetEmbeddingsIndex" + id_postfix,
        vector_bucket_name=dataset_embeddings.vector_bucket_name,
        index_name="dataset-embeddings-index" + name_postfix,
        data_type="float32",
        dimension=EMBEDDING_DIMENSION,
        distance_metric="cosine",
    )
    dataset_embeddings_index.node.add_dependency(dataset_embeddings)

    NagSuppressions.add_resource_suppressions(
        dataset_embeddings.node.find_child("S3VectorsBucketHandler"),
        [
            {"id": "AwsSolutions-IAM4", "reason": "AWS managed policy used by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-IAM5", "reason": "Wildcard permissions required by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-L1", "reason": "Lambda runtime managed by cdk-s3-vectors construct"}
        ],
        apply_to_children=True
    )

    NagSuppressions.add_resource_suppressions(
        dataset_embeddings.node.find_child("S3VectorsProvider"),
        [
            {"id": "AwsSolutions-IAM4", "reason": "AWS managed policy used by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-IAM5", "reason": "Wildcard permissions required by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-L1", "reason": "Lambda runtime managed by cdk-s3-vectors construct"}
        ],
        apply_to_children=True
    )

    NagSuppressions.add_resource_suppressions(
        dataset_embeddings_index,
        [
            {"id": "AwsSolutions-IAM4", "reason": "AWS managed policy used by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-IAM5", "reason": "Wildcard permissions required by cdk-s3-vectors construct"},
            {"id": "AwsSolutions-L1", "reason": "Lambda runtime managed by cdk-s3-vectors construct"}
        ],
        apply_to_children=True
    )

    
    ssm.StringParameter(
        stack, "VectorDB_Bucket" + name_postfix,
        parameter_name="/data-analyst/vectordb_bucket" + name_postfix,
        string_value=dataset_embeddings.vector_bucket_name
    )
    ssm.StringParameter(
        stack, "VectorDB_Index" + name_postfix,
        parameter_name="/data-analyst/vectordb_index" + name_postfix,
        string_value=dataset_embeddings_index.index_name
    )

    return dataset_embeddings


class DataStack(Stack):
    def __init__(
        self, 
        scope: Construct, 
        construct_id: str,
        **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Parameters of the embeddings.
        # They have to be consistent across: index creation and runtime search
        self.embedder = DEFAULT_EMBEDDER
        ssm.StringParameter(
            self, "VectorDB_EmbedderParam",
            parameter_name="/data-analyst/vectordb_embedder",
            string_value=self.embedder
        )
        self.embedding_dimension = str(EMBEDDING_DIMENSION)
        ssm.StringParameter(
            self, "VectorDB_DimensionParam",
            parameter_name="/data-analyst/vectordb_dimension",
            string_value=self.embedding_dimension
        )

        # Prod DB
        self.dataset_embeddings = construct_vector_db(self)

        # Dev DB for experiments
        construct_vector_db(self, 'dev')

        # S3 bucket for access logs
        self.access_logs_bucket = s3.Bucket(
            self, "AccessLogsBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY
        )
        NagSuppressions.add_resource_suppressions(
            self.access_logs_bucket,
            [{"id": "AwsSolutions-S1", "reason": "Access logs bucket does not need access logging itself"}]
        )

        # S3 bucket for parquet files
        self.athena_data_bucket = s3.Bucket(
            self, "AthenaDataBucket",
            bucket_name=f"datasets-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix="athena-data-bucket/"
        )

        # S3 bucket name parameter to be used by scripts
        ssm.StringParameter(
            self, "DataBucketParam",
            parameter_name="/data-analyst/data-bucket",
            string_value=self.athena_data_bucket.bucket_name
        )

        # S3 bucket for Athena query results
        self.athena_query_results_bucket = s3.Bucket(
            self, "AthenaQueryResultsBucket",
            bucket_name=f"athena-query-results-{self.account}-{self.region}",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            server_access_logs_bucket=self.access_logs_bucket,
            server_access_logs_prefix="athena-query-results/"
        )

        ssm.StringParameter(
            self, "AthenaQueryResultsBucketParam",
            parameter_name="/data-analyst/athena-query-results-bucket",
            string_value=self.athena_query_results_bucket.bucket_name
        )

        # Glue Database for Athena tables
        self.glue_database = glue.CfnDatabase(
            self, "DatasetsGlueDatabase",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="datasets",
                description="Database for dataset tables queryable via Athena"
            )
        )

        # Glue metadata table for dataset metadata JSON files
        datasets_metadata_table = glue.CfnTable(
            self, "DatasetsMetadataTable",
            catalog_id=self.account,
            database_name=self.glue_database.ref,
            table_input=glue.CfnTable.TableInputProperty(
                name="datasets_metadata",
                description="Metadata table for dataset information from JSON files",
                table_type="EXTERNAL_TABLE",
                parameters={
                    "EXTERNAL": "TRUE"
                },
                storage_descriptor=glue.CfnTable.StorageDescriptorProperty(
                    columns=[
                        glue.CfnTable.ColumnProperty(
                            name="dataset_id",
                            type="string",
                            comment="Dataset identifier"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="title",
                            type="string",
                            comment="Dataset title"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="description",
                            type="string",
                            comment="Dataset description"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="keywords",
                            type="array<string>",
                            comment="Dataset keywords"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="last_updated",
                            type="string",
                            comment="Last update timestamp"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="unit_of_measure",
                            type="string",
                            comment="Unit of measurement"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="survey",
                            type="string",
                            comment="Survey name"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="national_statistic",
                            type="boolean",
                            comment="Whether this is a national statistic"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="state",
                            type="string",
                            comment="Publication state"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="type",
                            type="string",
                            comment="Dataset type"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="canonical_topic",
                            type="string",
                            comment="Canonical topic identifier"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="subtopics",
                            type="array<string>",
                            comment="Subtopic identifiers"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="contacts",
                            type="array<struct<name:string,email:string,telephone:string>>",
                            comment="Contact information"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="dimensions",
                            type="array<struct<id:string,label:string,description:string,number_of_options:int,is_area_type:boolean,variable:string>>",
                            comment="Dataset dimensions"
                        ),
                        glue.CfnTable.ColumnProperty(
                            name="related_content",
                            type="array<struct<title:string,description:string,href:string>>",
                            comment="Related content links"
                        ),
                    ],
                    location=f"s3://{self.athena_data_bucket.bucket_name}/datasets-metadata/",
                    input_format="org.apache.hadoop.mapred.TextInputFormat",
                    output_format="org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
                    serde_info=glue.CfnTable.SerdeInfoProperty(
                        serialization_library="org.openx.data.jsonserde.JsonSerDe",
                        parameters={
                            "serialization.format": "1",
                            "ignore.malformed.json": "true"
                        }
                    )
                )
            )
        )
        datasets_metadata_table.node.add_dependency(self.glue_database)

        # Athena Workgroup for dataset queries
        athena_workgroup = athena.CfnWorkGroup(
            self, "AthenaWorkgroup",
            name="datasets-workgroup",
            description="Workgroup for querying datasets with cost tracking and optimization",
            state="ENABLED",
            work_group_configuration=athena.CfnWorkGroup.WorkGroupConfigurationProperty(
                result_configuration=athena.CfnWorkGroup.ResultConfigurationProperty(
                    output_location=f"s3://{self.athena_query_results_bucket.bucket_name}/",
                    encryption_configuration=athena.CfnWorkGroup.EncryptionConfigurationProperty(
                        encryption_option="SSE_S3"
                    )
                ),
                enforce_work_group_configuration=True,
                publish_cloud_watch_metrics_enabled=True,
                engine_version=athena.CfnWorkGroup.EngineVersionProperty(
                    selected_engine_version="AUTO"
                )
            ),
            tags=[
                {
                    "key": "Project",
                    "value": "Datasets"
                },
                {
                    "key": "Environment",
                    "value": "Development"
                },
                {
                    "key": "CostCenter",
                    "value": "DataAnalytics"
                }
            ]
        )
        athena_workgroup.node.add_dependency(self.athena_query_results_bucket)

        # Lambda function to automatically create Glue tables when Parquet files are uploaded
        lambda_dir = Path(__file__).parent.parent / "lambda"

        # IAM role for Lambda with Glue and S3 permissions
        lambda_role = iam.Role(
            self, "GlueTableCreatorLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            description="Execution role for Glue table creator Lambda function"
        )
        
        # Grant Lambda permissions to read from S3
        self.athena_data_bucket.grant_read(lambda_role)
        
        # Grant Lambda permissions to create/update Glue tables
        lambda_role.add_to_policy(iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                "glue:CreateTable",
                "glue:UpdateTable",
                "glue:GetTable",
                "glue:GetDatabase",
                "glue:CreatePartition",
                "glue:GetPartitions",
                "glue:GetPartition"
            ],
            resources=[
                f"arn:aws:glue:{self.region}:{self.account}:catalog",
                f"arn:aws:glue:{self.region}:{self.account}:database/{self.glue_database.ref}",
                f"arn:aws:glue:{self.region}:{self.account}:table/{self.glue_database.ref}/*"
            ]
        ))

        NagSuppressions.add_resource_suppressions(
            lambda_role,
            [
                {"id": "AwsSolutions-IAM4", "reason": "AWSLambdaBasicExecutionRole is required for CloudWatch Logs"},
                {"id": "AwsSolutions-IAM5", "reason": "S3 read permissions and Glue table wildcard required for dynamic table creation"}
            ],
            apply_to_children=True
        )

        # This separates the heavy pyarrow dependency from the function code
        # For Python 3.13: arn:aws:lambda:region:336392948345:layer:AWSSDKPandas-Python313:5
        pyarrow_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, "PyArrowLayer",
            layer_version_arn=f"arn:aws:lambda:{self.region}:336392948345:layer:AWSSDKPandas-Python313:5"
        )

        glue_table_creator = lambda_.Function(
            self, "GlueTableCreatorFunction",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="create_glue_table.lambda_handler",
            code=lambda_.Code.from_asset(
                str(lambda_dir / "parse_dataset"),
                exclude=["requirements.txt", "__pycache__"]
            ),
            role=lambda_role,
            layers=[pyarrow_layer],
            timeout=Duration.minutes(15), # Keeping the Lambda duration maximum for large parquet files
            memory_size=10240,  # Keeping the memory high for large parquet files
            environment={
                "GLUE_DATABASE_NAME": self.glue_database.ref,
                "BUCKET_NAME": self.athena_data_bucket.bucket_name
            },
            description="Automatically creates Glue tables when new Parquet files are uploaded"
        )

        NagSuppressions.add_resource_suppressions(
            glue_table_creator,
            [{"id": "AwsSolutions-L1", "reason": "Python 3.13 used for AWSSDKPandas Lambda layer compatibility"}]
        )
        
        # S3 event notification to trigger Lambda for .parquet files in datasets/ prefix
        self.athena_data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(glue_table_creator),
            s3.NotificationKeyFilter(
                prefix="datasets/",
                suffix=".parquet"
            )
        )

        # IAM role for Vector DB Lambda indexer
        vector_db_lambda_role = iam.Role(
            self, "VectorDB_IndexerLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSLambdaBasicExecutionRole")
            ],
            description="Execution role for Vector DB indexer Lambda function"
        )
        # Grant Lambda permissions to read from S3
        self.athena_data_bucket.grant_read(vector_db_lambda_role)

        # Grant Lambda permissions to index dataset
        vector_db_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3vectors:QueryVectors", 
                    "s3vectors:GetVectors",
                    "s3vectors:CreateIndex",
                    "s3vectors:GetIndex",
                    "s3vectors:ListIndexes",
                    "s3vectors:PutVectors"
                ],
                resources=[
                    f"arn:aws:s3vectors:{self.region}:{self.account}:bucket/{self.dataset_embeddings.vector_bucket_name}/index/*"
                ]
            ),
        )

        # Grant Lambda permissions to invoke Bedrock embedding models
        vector_db_lambda_role.add_to_policy(
            iam.PolicyStatement(
                sid="BedrockModelInvocation",
                effect=iam.Effect.ALLOW,
                actions=[
                    "bedrock:InvokeModel",
                    "bedrock:InvokeModelWithResponseStream"
                ],
                resources=[
                    "arn:aws:bedrock:*::foundation-model/*",
                    f"arn:aws:bedrock:{self.region}:{self.account}:*"
                ]
            )
        )
        vector_db_lambda_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "ssm:GetParameter", 
                    "ssm:GetParameters"
                ],
                resources=[
                    f"arn:aws:ssm:{self.region}:{self.account}:parameter/data-analyst/*"
                ]
            )
        )

        NagSuppressions.add_resource_suppressions(
            vector_db_lambda_role,
            [
                {"id": "AwsSolutions-IAM4", "reason": "AWSLambdaBasicExecutionRole is required for CloudWatch Logs"},
                {"id": "AwsSolutions-IAM5", "reason": "Wildcards required for S3 read, S3 Vectors index operations, Bedrock foundation models, and SSM parameters"}
            ],
            apply_to_children=True
        )


        vector_db_indexer = lambda_.Function(
            self, "VectorDB_IndexerFunction",
            runtime=lambda_.Runtime.PYTHON_3_14,
            handler="indexer.lambda_handler",
            code=lambda_.Code.from_asset(
                str(lambda_dir / "indexer_dataset"),
                exclude=["requirements.txt", "__pycache__"]
            ),
            role=vector_db_lambda_role,
            environment={
                "BUCKET_NAME": self.athena_data_bucket.bucket_name
            },
            description="Automatically index datasets when new metadata files are uploaded"
        )
        
        # S3 event notification to trigger Lambda for .parquet files in datasets/ prefix
        self.athena_data_bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.LambdaDestination(vector_db_indexer),
            s3.NotificationKeyFilter(
                prefix="metadata/",
                suffix=".json"
            )
        )

        NagSuppressions.add_resource_suppressions_by_path(
            self,
            f"/{self.stack_name}/BucketNotificationsHandler050a0587b7544547bf325f094a3db834/Role/Resource",
            [{"id": "AwsSolutions-IAM4", "reason": "CDK internal BucketNotificationsHandler requires AWSLambdaBasicExecutionRole"}]
        )