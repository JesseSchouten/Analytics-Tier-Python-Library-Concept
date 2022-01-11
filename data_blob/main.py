from BlobContainer import BlobContainerDCInternal

blob = BlobContainerDCInternal(spark=None)

options = {
    "spark.delimiter" : ";"
  }

print(blob.select_from_csv('dclov', "example/path", options =  options))
