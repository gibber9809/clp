import * as cdk from "aws-cdk-lib";
import * as eks from "aws-cdk-lib/aws-eks";
import * as iam from "aws-cdk-lib/aws-iam";
import * as s3 from "aws-cdk-lib/aws-s3";
import { NagSuppressions } from "cdk-nag";
import { Construct } from "constructs";

export interface ClpStackProps extends cdk.StackProps {
  cluster: eks.Cluster;
  archiveBucket: s3.IBucket;
}

export class ClpStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ClpStackProps) {
    super(scope, id, props);

    const { cluster, archiveBucket } = props;

    // Create an IRSA (IAM Roles for Service Accounts) role for S3 access.
    // Use CfnJson to avoid token-in-map-key resolution errors.
    const oidcProviderArn =
      cluster.openIdConnectProvider.openIdConnectProviderArn;
    // Strip "https://" — IAM OIDC condition keys use the issuer without the scheme.
    const oidcIssuer = cdk.Fn.select(
      1,
      cdk.Fn.split("://", cluster.clusterOpenIdConnectIssuerUrl)
    );

    const conditionKey = new cdk.CfnJson(this, "OidcConditionKey", {
      value: {
        [`${oidcIssuer}:sub`]: "system:serviceaccount:clp:clp-service-account",
        [`${oidcIssuer}:aud`]: "sts.amazonaws.com",
      },
    });

    const s3AccessRole = new iam.Role(this, "ClpS3AccessRole", {
      assumedBy: new iam.FederatedPrincipal(
        oidcProviderArn,
        {
          StringEquals: conditionKey,
        },
        "sts:AssumeRoleWithWebIdentity"
      ),
    });

    s3AccessRole.addToPolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket",
        ],
        resources: [archiveBucket.bucketArn, `${archiveBucket.bucketArn}/*`],
      })
    );

    // Deploy CLP Helm chart from the published chart repository.
    const s3Config = {
      region_code: this.region,
      bucket: archiveBucket.bucketName,
      key_prefix: "archives/",
      aws_authentication: { type: "default" },
    };

    new eks.HelmChart(this, "ClpChart", {
      cluster,
      chart: "clp",
      repository: "https://y-scope.github.io/clp",
      version: "0.2.1-dev.1",
      namespace: "clp",
      createNamespace: true,
      values: {
        fullnameOverride: "clp",
        distributedDeployment: true,
        clpConfig: {
          logs_input: {
            type: "s3",
            aws_authentication: { type: "default" },
          },
          archive_output: {
            storage: {
              type: "s3",
              s3_config: s3Config,
            },
          },
          stream_output: {
            storage: {
              type: "s3",
              s3_config: {
                ...s3Config,
                key_prefix: "streams/",
              },
            },
          },
        },
        serviceAccount: {
          annotations: {
            "eks.amazonaws.com/role-arn": s3AccessRole.roleArn,
          },
        },
      },
    });

    new cdk.CfnOutput(this, "S3AccessRoleArn", {
      value: s3AccessRole.roleArn,
      description: "IRSA role ARN for CLP service account S3 access",
    });

    new cdk.CfnOutput(this, "ArchiveBucketName", {
      value: archiveBucket.bucketName,
      description: "S3 bucket for CLP archives and streams",
    });

    NagSuppressions.addStackSuppressions(
      this,
      [
        {
          id: "AwsSolutions-IAM5",
          reason:
            "IRSA role needs wildcard permissions on S3 bucket objects (bucket/*).",
        },
        {
          id: "AwsSolutions-IAM4",
          reason: "Managed policies used by EKS Helm chart lambda handler.",
        },
        {
          id: "AwsSolutions-L1",
          reason:
            "Lambda runtime version is managed by the CDK EKS construct.",
        },
      ],
      true
    );
  }
}
