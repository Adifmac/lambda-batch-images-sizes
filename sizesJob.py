import boto3
import urllib
from urllib.parse import unquote_plus, quote_plus, urlencode
from botocore.exceptions import ClientError
import os
import uuid
import logging
from PIL import Image
import PIL.Image

# Instantiate boto client
s3_client = boto3.client('s3')


def lambda_handler(event, context):
    # Parse job parameters from S3 Batch Operations
    jobId = event['job']['id']
    invocationId = event['invocationId']
    invocationSchemaVersion = event['invocationSchemaVersion']

    # Prepare results
    results = []

    # Parse Amazon S3 Key, Key Version, and Bucket ARN
    taskId = event['tasks'][0]['taskId']
    s3Key = event['tasks'][0]['s3Key']
    s3VersionId = event['tasks'][0]['s3VersionId']
    s3BucketArn = event['tasks'][0]['s3BucketArn']
    s3Bucket = s3BucketArn.split(':::')[-1]

    # Prepare result code and string
    resultCode = 'Succeeded'
    resultString = 'file was skipped'

    if is_photo_valid(s3Key):
        # this is a big image, define vars for process:
        mini_key = s3Key.rsplit('/', 1).pop()
        thumb_key = get_thumb_name(s3Key)
        med_key = get_med_name(s3Key)
        download_path = '/tmp/{}{}'.format(uuid.uuid4(), mini_key)
        t_upload_path = '/tmp/resized-{}'.format(mini_key)
        m_upload_path = '/tmp/resized_med-{}'.format(mini_key)

        # Resize image:
        try:
            fixed_key = urllib.parse.unquote_plus(s3Key)
            s3_client.download_file(s3Bucket, fixed_key, download_path)
            resize_image(download_path, t_upload_path, m_upload_path)
            s3_client.upload_file(t_upload_path, s3Bucket, thumb_key,
                                  ExtraArgs={'ContentType': 'image/jpeg', 'ACL': 'public-read'})
            s3_client.upload_file(m_upload_path, s3Bucket, med_key,
                                  ExtraArgs={'ContentType': 'image/jpeg', 'ACL': 'public-read'})
            # Mark as succeeded
            resultCode = 'Succeeded'
            resultString = 'thumb and medium created'

        except ClientError as e:
            # If request timed out, mark as a temp failure and S3 Batch Operations will make the task for retry.
            # If any other exceptions are received, mark as permanent failure.
            errorCode = e.response['Error']['Code']
            errorMessage = e.response['Error']['Message']
            if errorCode == 'RequestTimeout':
                resultCode = 'TemporaryFailure'
                resultString = 'Retry request to Amazon S3 due to timeout.'
            else:
                resultCode = 'PermanentFailure'
                resultString = '{}: {}'.format(errorCode, errorMessage)
        except Exception as e:
            # Catch all exceptions to permanently fail the task
            resultCode = 'PermanentFailure'
            resultString = 'Exception: {}'.format(e)
        finally:
            # Delete temporary files
            delete_tmp_files(download_path, t_upload_path, m_upload_path)

    results.append({
        'taskId': taskId,
        'resultCode': resultCode,
        'resultString': resultString
    })

    return {
        'invocationSchemaVersion': invocationSchemaVersion,
        'treatMissingKeysAs': 'PermanentFailure',
        'invocationId': invocationId,
        'results': results
    }
    
    
def is_photo_valid(key):
    if key.lower().endswith(('/', '.svg', '.js')):
        return False
    key_end = key.rsplit('/', 1).pop()
    if key_end.find('bwt_') == 0:
        return False
    elif key_end.find('thumb_') == 0:
        return False
    elif key_end.find('m3m_') == 0:
        return False
    # elif key_end.lower().endswith(('.png', '.jpg', '.jpeg'))
    return True


def get_thumb_name(orig_name):
    orig_name = unquote_plus(orig_name)
    if '/' in orig_name:
        folders = orig_name.rsplit('/', 1)
        key_end = folders.pop()
        return folders[0] + '/thumb_' + key_end
    else:
        return 'thumb_' + orig_name


def get_med_name(orig_name):
    orig_name = unquote_plus(orig_name)
    if '/' in orig_name:
        folders = orig_name.rsplit('/', 1)
        key_end = folders.pop()
        return folders[0] + '/m3m_' + key_end
    else:
        return 'm3m_' + orig_name


def resize_image(image_path, t_resized_path, m_resized_path):
    with Image.open(image_path) as image:
        orig_img = image.copy()
        image.thumbnail((700, 700), Image.ANTIALIAS)
        image.save(m_resized_path, optimize=True)

        image = orig_img.copy()
        image.thumbnail((220, 220), Image.ANTIALIAS)
        image.save(t_resized_path, optimize=True)


def delete_tmp_files(download_path, t_upload_path, m_upload_path):
    try:
        os.remove(download_path)
    except OSError:
        pass

    try:
        os.remove(t_upload_path)
    except OSError:
        pass

    try:
        os.remove(m_upload_path)
    except OSError:
        pass

