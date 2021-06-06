An AWS Lambda function for AWS S3 batch operation, to create thumbnails and/or reduced size for png and jpg images.

Resized sizes:

thumb = 200x200

m5m = 1100x1100

m4m = 900x900

m3m = 700x700

m2m = 500x500

m1m = 300x300 

<!> All resized images are added to the same location as the original image. 

Using https://github.com/jamesacampbell/iptcinfo3 for IPTC processing.

Using pillow for image processing - how to create a layer for aws lambda: https://towardsdatascience.com/python-packages-in-aws-lambda-made-easy-8fbc78520e30
