import { beforeEach, describe, expect, test } from "vitest";
import * as cdk from "aws-cdk-lib";
import * as ec2 from "aws-cdk-lib/aws-ec2";
import * as eks from "aws-cdk-lib/aws-eks";
import * as s3 from "aws-cdk-lib/aws-s3";
import { Annotations, Match, Template } from "aws-cdk-lib/assertions";
import { KubectlV32Layer } from "@aws-cdk/lambda-layer-kubectl-v32";
import { AwsSolutionsChecks, NagSuppressions } from "cdk-nag";
import { ClpStack } from "../lib/clp-stack";

/**
 * Creates a real EKS cluster in the given stack so that OIDC and kubectl
 * resources are available for the ClpStack under test.
 */
function createTestCluster(stack: cdk.Stack): eks.Cluster {
  const vpc = new ec2.Vpc(stack, "Vpc");
  return new eks.Cluster(stack, "TestCluster", {
    vpc,
    version: eks.KubernetesVersion.V1_32,
    kubectlLayer: new KubectlV32Layer(stack, "Kubectl"),
    defaultCapacity: 0,
  });
}

describe("ClpStack", () => {
  let app: cdk.App;
  let stack: ClpStack;
  let template: Template;

  beforeEach(() => {
    app = new cdk.App();
    const env = { account: "123456789012", region: "us-east-1" };

    const depStack = new cdk.Stack(app, "DepStack", { env });
    const cluster = createTestCluster(depStack);
    const archiveBucket = new s3.Bucket(depStack, "Bucket");

    stack = new ClpStack(app, "TestClp", { env, cluster, archiveBucket });
    template = Template.fromStack(stack);
  });

  test("IRSA role for S3 access is created", () => {
    template.hasResourceProperties("AWS::IAM::Role", {
      AssumeRolePolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: "sts:AssumeRoleWithWebIdentity",
          }),
        ]),
      }),
    });
  });

  test("IRSA role has S3 permissions for the archive bucket", () => {
    template.hasResourceProperties("AWS::IAM::Policy", {
      PolicyDocument: Match.objectLike({
        Statement: Match.arrayWith([
          Match.objectLike({
            Action: Match.arrayWith(["s3:GetObject", "s3:PutObject"]),
            Effect: "Allow",
          }),
        ]),
      }),
    });
  });

  test("Helm chart custom resource is created", () => {
    template.hasResourceProperties("Custom::AWSCDK-EKS-HelmChart", {
      Chart: "clp",
      Repository: "https://y-scope.github.io/clp",
      Version: "0.2.1-dev.1",
      Namespace: "clp",
      CreateNamespace: true,
    });
  });

  test("Helm chart values configure S3 storage", () => {
    const resources = template.findResources("Custom::AWSCDK-EKS-HelmChart");
    const helmChart = Object.values(resources)[0];
    // Values is a Fn::Join containing CDK tokens; verify the joined parts include S3 config
    const joinParts = helmChart?.Properties?.Values?.["Fn::Join"]?.[1];
    const valuesStr = JSON.stringify(joinParts);
    expect(valuesStr).toContain('\\"type\\":\\"s3\\"');
  });

  test("stack exports S3AccessRoleArn output", () => {
    template.hasOutput("S3AccessRoleArn", {});
  });

  test("stack exports ArchiveBucketName output", () => {
    template.hasOutput("ArchiveBucketName", {});
  });

  test("cdk-nag produces no errors", () => {
    const nagApp = new cdk.App();
    const env = { account: "123456789012", region: "us-east-1" };
    cdk.Aspects.of(nagApp).add(new AwsSolutionsChecks({ verbose: true }));

    const nagDepStack = new cdk.Stack(nagApp, "NagDep", { env });
    const cluster = createTestCluster(nagDepStack);
    const archiveBucket = new s3.Bucket(nagDepStack, "Bucket", {
      encryption: s3.BucketEncryption.S3_MANAGED,
      enforceSSL: true,
    });

    // Suppress nag issues on the dependency stack (not what we're testing)
    NagSuppressions.addStackSuppressions(
      nagDepStack,
      [
        { id: "AwsSolutions-IAM4", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-IAM5", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-EKS1", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-L1", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-S1", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-S10", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-SF1", reason: "Dependency stack for testing." },
        { id: "AwsSolutions-SF2", reason: "Dependency stack for testing." },
      ],
      true
    );

    const nagStack = new ClpStack(nagApp, "NagClp", {
      env,
      cluster,
      archiveBucket,
    });

    const errors = Annotations.fromStack(nagStack).findError(
      "*",
      Match.stringLikeRegexp("AwsSolutions-.*")
    );
    expect(errors).toHaveLength(0);
  });
});
