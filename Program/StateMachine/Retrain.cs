// Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.SageMaker;
using Amazon.SageMaker.Model;
using Amazon.SimpleSystemsManagement;
using Amazon.SimpleSystemsManagement.Model;
using DotStep.Common.Functions;
using DotStep.Core;

namespace Program.StateMachine
{
    public sealed class Retrain : StateMachine<Retrain.SetDefaults>
    {
        public class Context : IContext
        {
            [Required] public string TrainingFileCsv { get; set; }

            [Required] public string ResultsBucketName { get; set; }

            [Required] public string TrainingBucketName { get; set; }

            public string TrainingJobName { get; set; }
            public string TrainingImage { get; set; }

            [Required] public string TrainingRoleArn { get; set; }

            public string TrainingFileJson { get; set; }
            public string TrainingJobArn { get; set; }
            public string TrainingJobStatus { get; set; }

            public string ModelArn { get; set; }

            public string EndpointConfigArn { get; set; }

            public bool EndpointExists { get; set; }

            public string EndpointArn { get; set; }

            [Required] public string EndpointName { get; set; }

            [Required] public string Region { get; set; }
        }

        [DotStep.Core.Action(ActionName = "ssm:*")]
        public sealed class SetDefaults : TaskState<Context, Validate>
        {
            private readonly IAmazonSimpleSystemsManagement ssm = new AmazonSimpleSystemsManagementClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await ssm.GetParametersAsync(new GetParametersRequest
                {
                    Names = new List<string>
                    {
                        "/SalesForecast/BucketName",
                        "/SalesForecast/SageMakerRole",
                        "/SalesForecast/SageMakerTrainingContainer"
                    }
                });

                var defaultBucketName = result.Parameters.Single(p => p.Name == "/SalesForecast/BucketName").Value;
                var sageMakerRole = result.Parameters.Single(p => p.Name == "/SalesForecast/SageMakerRole").Value;
                var sageMakerContainer =
                    result.Parameters.Single(p => p.Name == "/SalesForecast/SageMakerTrainingContainer").Value;

                if (string.IsNullOrEmpty(context.ResultsBucketName))
                    context.ResultsBucketName = defaultBucketName;

                if (string.IsNullOrEmpty(context.TrainingBucketName))
                    context.TrainingBucketName = defaultBucketName;
                if (string.IsNullOrEmpty(context.TrainingRoleArn))
                    context.TrainingRoleArn = sageMakerRole;
                if (string.IsNullOrEmpty(context.TrainingImage))
                    context.TrainingImage = sageMakerContainer;
                if (string.IsNullOrEmpty(context.EndpointName))
                    context.EndpointName = "SalesForecast";
                if (string.IsNullOrEmpty(context.TrainingFileCsv))
                    context.TrainingFileCsv = "SageMaker/train.csv";
                if (string.IsNullOrEmpty(context.Region))
                    context.Region = "us-west-2";

                return context;
            }
        }

        public sealed class Validate : ReferencedTaskState<Context, TransformData,
            ValidateMessage<Context>>
        {
        }

        public sealed class Done : EndState
        {
        }

        public sealed class WaitForTrainingToComplete : WaitState<CheckTrainingStatus>
        {
            public override int Seconds => 30;
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class CheckTrainingStatus : TaskState<Context, DetermineNextTrainingStep>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var status = await sageMaker.DescribeTrainingJobAsync(new DescribeTrainingJobRequest
                {
                    TrainingJobName = context.TrainingJobName
                });
                context.TrainingJobStatus = status.TrainingJobStatus;

                return context;
            }
        }

        public sealed class DetermineNextTrainingStep : ChoiceState
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<WaitForTrainingToComplete, Context>(c => c.TrainingJobStatus == "InProgress"),
                new Choice<RegisterModel, Context>(c => c.TrainingJobStatus == "Completed")
            };
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class RegisterModel : TaskState<Context, CreateEndpointConfiguration>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateModelAsync(new CreateModelRequest
                {
                    ExecutionRoleArn = context.TrainingRoleArn,
                    ModelName = context.TrainingJobName,
                    PrimaryContainer = new ContainerDefinition
                    {
                        Image = context.TrainingImage,
                        ModelDataUrl =
                            $"https://s3-{context.Region}.amazonaws.com/{context.TrainingBucketName}/SageMaker/model/{context.TrainingJobName}/output/model.tar.gz"
                    }
                });

                context.ModelArn = result.ModelArn;
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class CreateEndpointConfiguration : TaskState<Context, GetEndpoint>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateEndpointConfigAsync(new CreateEndpointConfigRequest
                {
                    EndpointConfigName = context.TrainingJobName,
                    ProductionVariants = new List<ProductionVariant>
                    {
                        new ProductionVariant
                        {
                            InstanceType = ProductionVariantInstanceType.MlM4Xlarge,
                            InitialVariantWeight = 1,
                            InitialInstanceCount = 1,
                            ModelName = context.TrainingJobName,
                            VariantName = "AllTraffic"
                        }
                    }
                });

                context.EndpointConfigArn = result.EndpointConfigArn;
                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class GetEndpoint : TaskState<Context, DetermineIfEndpointExists>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                try
                {
                    var result = await sageMaker.DescribeEndpointAsync(new DescribeEndpointRequest
                    {
                        EndpointName = context.EndpointName
                    });

                    if (result.EndpointName == context.EndpointName)
                        context.EndpointExists = true;
                    else context.EndpointExists = false;
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    context.EndpointExists = false;
                }

                return context;
            }
        }

        public sealed class DetermineIfEndpointExists : ChoiceState
        {
            public override List<Choice> Choices => new List<Choice>
            {
                new Choice<CreateEndpoint, Context>(c => c.EndpointExists == false),
                new Choice<UpdateEndpoint, Context>(c => c.EndpointExists == true)
            };
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class CreateEndpoint : TaskState<Context, Done>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.CreateEndpointAsync(new CreateEndpointRequest
                {
                    EndpointName = context.EndpointName,
                    EndpointConfigName = context.TrainingJobName
                });

                context.EndpointArn = result.EndpointArn;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class UpdateEndpoint : TaskState<Context, Done>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                var result = await sageMaker.UpdateEndpointAsync(new UpdateEndpointRequest
                {
                    EndpointName = context.EndpointName,
                    EndpointConfigName = context.TrainingJobName
                });

                context.EndpointArn = result.EndpointArn;

                return context;
            }
        }

        [DotStep.Core.Action(ActionName = "sagemaker:*")]
        [DotStep.Core.Action(ActionName = "s3:*")]
        [DotStep.Core.Action(ActionName = "iam:*")]
        public sealed class SubmitTrainingJob : TaskState<Context, CheckTrainingStatus>
        {
            private readonly IAmazonSageMaker sageMaker = new AmazonSageMakerClient();

            public override async Task<Context> Execute(Context context)
            {
                context.TrainingRoleArn = context.TrainingRoleArn;
                context.TrainingJobName = $"SalesForecast-{DateTime.UtcNow.Ticks}";


                var result = await sageMaker.CreateTrainingJobAsync(new CreateTrainingJobRequest
                {
                    TrainingJobName = context.TrainingJobName,
                    AlgorithmSpecification = new AlgorithmSpecification
                    {
                        TrainingInputMode = TrainingInputMode.File,
                        TrainingImage = context.TrainingImage
                    },
                    RoleArn = context.TrainingRoleArn,
                    OutputDataConfig = new OutputDataConfig
                    {
                        S3OutputPath = $"s3://{context.TrainingBucketName}/SageMaker/model"
                    },
                    ResourceConfig = new ResourceConfig
                    {
                        InstanceCount = 1,
                        InstanceType = TrainingInstanceType.MlM4Xlarge,
                        VolumeSizeInGB = 5
                    },
                    StoppingCondition = new StoppingCondition
                    {
                        MaxRuntimeInSeconds = Convert.ToInt32(TimeSpan.FromHours(1).TotalSeconds)
                    },
                    HyperParameters = new Dictionary<string, string>
                    {
                        {"context_length", "72"},
                        {"dropout_rate", "0.05"},
                        {"early_stopping_patience", "10"},
                        {"epochs", "20"},
                        {"learning_rate", "0.001"},
                        {"likelihood", "gaussian"},
                        {"mini_batch_size", "32"},
                        {"num_cells", "40"},
                        {"num_layers", "3"},
                        {"prediction_length", "90"},
                        {"time_freq", "D"}
                    },
                    InputDataConfig = new List<Channel>
                    {
                        new Channel
                        {
                            ChannelName = "train",
                            CompressionType = CompressionType.None,
                            DataSource = new DataSource
                            {
                                S3DataSource = new S3DataSource
                                {
                                    S3DataType = S3DataType.S3Prefix,
                                    S3DataDistributionType = S3DataDistribution.FullyReplicated,
                                    S3Uri = $"s3://{context.TrainingBucketName}/{context.TrainingFileJson}"
                                }
                            }
                        }
                    }
                });

                context.TrainingJobArn = result.TrainingJobArn;

                return context;
            }
        }

        [FunctionMemory(Memory = 3008)]
        [FunctionTimeout(Timeout = 300)]
        [DotStep.Core.Action(ActionName = "s3:*")]
        public sealed class TransformData : TaskState<Context, SubmitTrainingJob>
        {
            private readonly IAmazonS3 s3 = new AmazonS3Client();

            public string ConvertToJsonLines(string csv)
            {
                var rows = csv.Split('\n')
                    .Skip(1)
                    .Where(row => row != "")
                    .ToList();

                var totalStores = rows.Select(r => r.Split(',')[1]).Distinct().Count();
                var totalItems = rows.Select(r => r.Split(',')[2]).Distinct().Count();

                var sb = new StringBuilder();

                var category = 0;
                for (var store = 1; store <= totalStores; store++)
                for (var item = 1; item <= totalItems; item++)
                {
                    sb.Append("{\"start\": \"2013-01-01 00:00:00\", \"target\": [");

                    var sales = rows.Where(
                            row =>
                                Convert.ToInt32(row.Split(',')[1]) == store &&
                                Convert.ToInt32(row.Split(',')[2]) == item)
                        .Select(row => new
                        {
                            Sales = row.Split(',')[3]
                        })
                        .ToList();

                    for (var i = 0; i < sales.Count(); i++)
                    {
                        if (i > 0)
                            sb.Append(", ");
                        sb.Append(sales[i].Sales);
                    }

                    sb.AppendLine($"], \"cat\": [{category}]}}");
                    category++;
                }

                return sb.ToString();
            }

            public Stream StringToStream(string stringData)
            {
                var uniEncoding = new UTF8Encoding();
                var ms = new MemoryStream();
                var sw = new StreamWriter(ms, uniEncoding);
                sw.Write(stringData);
                sw.Flush();
                ms.Seek(0, SeekOrigin.Begin);
                return ms;
            }

            public override async Task<Context> Execute(Context context)
            {
                context.TrainingFileJson = "SageMaker/train.json";

                var getResult = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = context.ResultsBucketName,
                    Key = $"{context.TrainingFileCsv}"
                });

                using (var reader = new StreamReader(getResult.ResponseStream))
                {
                    var csv = reader.ReadToEndAsync().Result;
                    var jsonLines = ConvertToJsonLines(csv);
                    using (var stream = StringToStream(jsonLines))
                    {
                        await s3.PutObjectAsync(new PutObjectRequest
                        {
                            BucketName = context.TrainingBucketName,
                            Key = context.TrainingFileJson,
                            InputStream = stream,
                            ContentType = "application/json"
                        });
                    }
                }

                return context;
            }
        }
    }
}