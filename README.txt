# Project Setup Guide

This guide walks you through the process of setting up a Google Cloud Platform (GCP) project with a bucket, a virtual machine instance, and a service account.

## Step 1: Create a Bucket in Google Cloud Storage

1. Open the [Google Cloud Console](https://console.cloud.google.com/).
2. In the left navigation pane, navigate to **Storage** > **Browser**.
3. Click the **Create Bucket** button.
4. Follow the prompts to configure your bucket, specifying a unique name and adjusting settings as needed.
5. Click **Create**.

## Step 2: Create a Virtual Machine (Instance) in Compute Engine

1. In the Google Cloud Console, navigate to **Compute Engine**.
2. Click the **Create Instance** button.
3. Configure your instance settings, such as name, region, machine type, and boot disk.
4. Click **Create**.

## Step 3: Create a Service Account and Obtain Project ID

1. In the Google Cloud Console, navigate to **IAM & Admin** > **Service accounts**.
2. Click **Create Service Account**.
3. Fill out the form, granting the necessary roles (e.g., Storage Admin, Compute Admin).
4. Click **Create** to create the service account.
5. After creating the service account, note the **Project ID** from the main dashboard.

Congratulations! You've successfully set up a GCP project with a bucket, a virtual machine instance, and a service account.

For more detailed information and advanced configurations, refer to the official [Google Cloud Documentation](https://cloud.google.com/docs).

# Terraform Infrastructure Deployment

This Terraform configuration deploys a basic infrastructure on Google Cloud Platform (GCP) that includes a custom VPC network, subnetwork, router, NAT, route for internet access, and a compute instance.

## Prerequisites

- Install [Terraform](https://www.terraform.io/downloads.html).
- Authenticate with Google Cloud by setting up [Google Cloud credentials](https://registry.terraform.io/providers/hashicorp/google/latest/docs/guides/provider_reference#full-reference).

## Configuration Details

### Variables

- `project_id` (Type: String): The Google Cloud Project ID.

### Provider Configuration

This Terraform configuration uses the `google` provider to manage GCP resources. The following settings are specified:

- `project`: The GCP project where resources will be created.
- `region`: The default region for resources (set to "us-central1").
- `zone`: The default zone for resources (set to "us-central1-b").

### Resources

#### 1. Google Project Service

- Service: `compute.googleapis.com`

#### 2. Google Compute Network

- Name: `terraform-network`
- Auto Create Subnetworks: `false`
- Delete Default Routes on Create: `true`

#### 3. Google Compute Subnetwork

- Name: `private-network`
- IP CIDR Range: `10.2.0.0/16`
- Network: Reference to the `terraform-network`

#### 4. Google Compute Router

- Name: `quickstart-router`
- Network: Reference to the `terraform-network`

#### 5. Google Compute Router NAT

- Name: `quickstart-router-nat`
- Router: Reference to the `quickstart-router`
- Region: Reference to the `quickstart-router`

#### 6. Google Compute Route

- Name: `private-network-internet`
- Destination Range: `0.0.0.0/0`
- Network: Reference to the `terraform-network`
- Next Hop Gateway: `default-internet-gateway`
- Priority: `100`

#### 7. Google Compute Instance

- Name: `nginx-instance`
- Machine Type: `f1-micro`
- Tags: `nginx-instance`
- Boot Disk: CentOS 7 image

## Usage

1. Set the `project_id` variable in the `variables.tf` file.
2. Run the following commands in the terminal:

    ```bash
    terraform init
    terraform apply
    ```

3. Review the planned changes and type `yes` to apply the changes.
4. After deployment, you can check your GCP Console for the created resources.

## Cleanup

To avoid unnecessary charges, run the following command to destroy the created resources:

```bash
terraform destroy

