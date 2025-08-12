module "network" {
  source     = "../../modules/network"
  name       = "starter-prod"
  cidr_block = "10.20.0.0/16"
}
