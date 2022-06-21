# go-cosmos

A sample app using the [Azure Cosmos DB SDK for Go](https://pkg.go.dev/github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos) with Cosmos DB (SQL).

## Connect with NewDefaultAzureCredential

For most use cases you will use the `azidentity.NewDefaultAzureCredential` which will automatically authenticate across a range of options from local Azure CLI (during development) to Managed Identity (in production) without account keys.

You will need to create and assign a role using the bash snippets below, or the script at [data/role-assign-create.sh](./data/role-assign-create.sh).

```bash
RESOURCE_GROUP='220600-cosmos-db'
COSMOS_ACCOUNT_NAME='cosmos220600'

export AZURE_COSMOS_ENDPOINT="https://${COSMOS_ACCOUNT_NAME}.documents.azure.com:443/"

go run .
```

## Connect with Cosmos DB Account Key

If you are using this CLI to automatically create your databases (`database-v*`), via command `k`, this is a control plane, rather than a data plane operation, and you will need to temporarily use the Cosmos DB account key.

This CLI will automatically create a client with `azcosmos.NewClientWithKey` rather than `azidentity.NewDefaultAzureCredential`, if the `AZURE_COSMOS_KEY` is populated.

However, it is more secure to use the `azidentity.NewDefaultAzureCredential` option above in production.

```bash
RESOURCE_GROUP='220600-cosmos-db'
COSMOS_ACCOUNT_NAME='cosmos220600'

export AZURE_COSMOS_ENDPOINT="https://${COSMOS_ACCOUNT_NAME}.documents.azure.com:443/"

export AZURE_COSMOS_KEY="$(az cosmosdb keys list \
    --resource-group $RESOURCE_GROUP \
    --name $COSMOS_ACCOUNT_NAME \
    --out tsv \
    --query primaryMasterKey)"

go run .
```

## Create and Assign Role for Logged-in Azure CLI user

Use script at [data/role-assign-create.sh](./data/role-assign-create.sh) or paste the below into your terminal.

Update the values for `RESOURCE_GROUP` and `COSMOS_ACCOUNT_NAME` as neccessary.

```bash
cd data/

RESOURCE_GROUP='220600-cosmos-db'
COSMOS_ACCOUNT_NAME='cosmos220600'

export AZURE_COSMOS_ENDPOINT="https://${COSMOS_ACCOUNT_NAME}.documents.azure.com:443/"

AD_OBJECT_ID="$(az ad signed-in-user show --out tsv --query id)"

EXISTS=$(az cosmosdb sql role definition list \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --out tsv \
    --query '[?roleName == `MyReadWriteRole`] | [0].id')

if [[ -z "${EXISTS:-}" ]] then
echo "role definition does not exist"
az cosmosdb sql role definition create \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --body @role-definition-rw.json
else
echo "role definition exists"
fi

ROLE_DEFINITION_ID=$(az cosmosdb sql role definition list \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --out tsv \
    --query '[?roleName == `MyReadWriteRole`] | [0].id')

az cosmosdb sql role assignment create \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --role-definition-id $ROLE_DEFINITION_ID \
    --principal-id $AD_OBJECT_ID \
    --scope "/"
```
