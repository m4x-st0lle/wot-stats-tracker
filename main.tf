# 1. Den Azure Provider definieren
terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {} # Erforderlich für den AzureRM Provider
}

# 2. Eine Ressourcengruppe definieren (falls noch nicht vorhanden)
resource "azurerm_resource_group" "vv-dataflow-rg" {
  name     = "vv-dataflow-rg"
  location = "westeurope"
}

# 3. Das neue Speicherkonto anlegen
resource "azurerm_storage_account" "myspace" {
  name                     = "vv-dataflow" # Muss weltweit(!) eindeutig sein
  resource_group_name      = azurerm_resource_group.vv-dataflow-rg.name
  location                 = azurerm_resource_group.vv-dataflow-rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS" # Lokal redundant (günstigste Option)

  tags = {
    environment = "staging"
  }
}