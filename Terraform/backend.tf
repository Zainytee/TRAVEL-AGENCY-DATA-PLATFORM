terraform{
    backend "s3" {
      bucket = "ztf-state-bucket-omot-2024"
      region = "eu-central-1"
      key = "product/product.tfstate"
    }
}