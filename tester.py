import boto3
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# ✅ Replace with your actual bucket name
BUCKET_NAME = "yetal"

def test_s3_connection():
    print("🔍 Testing S3 connection...\n")

    try:
        s3 = boto3.client("s3")
        # Try to access the bucket
        s3.head_bucket(Bucket=BUCKET_NAME)
        print(f"✅ Successfully connected to bucket: {BUCKET_NAME}")

        # Optionally list a few files to confirm access
        response = s3.list_objects_v2(Bucket=BUCKET_NAME, MaxKeys=5)
        print("\n📂 Files in bucket:")
        if "Contents" in response:
            for obj in response["Contents"]:
                print(f"  • {obj['Key']}")
        else:
            print("  (No files found in bucket.)")

    except NoCredentialsError:
        print("❌ No AWS credentials found. Make sure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are set.")
    except EndpointConnectionError as e:
        print(f"❌ Could not connect to the S3 endpoint: {e}")
    except ClientError as e:
        error_code = e.response["Error"]["Code"]
        if error_code == "403":
            print("❌ Access denied (403 Forbidden). Your keys might be invalid or lack permission for this bucket.")
        elif error_code == "404":
            print("❌ Bucket not found (404). The bucket name may be incorrect or in a different region.")
        else:
            print(f"❌ AWS ClientError: {error_code} - {e.response['Error']['Message']}")
    except Exception as e:
        print(f"❌ Unexpected error: {e}")

if __name__ == "__main__":
    test_s3_connection()
