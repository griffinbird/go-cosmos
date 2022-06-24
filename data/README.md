# README

## Create and Assign Role for Logged-in Azure CLI user

Use script at [data/role-assign-create.sh](./data/role-assign-create.sh).

Update the values for `RESOURCE_GROUP` and `COSMOS_ACCOUNT_NAME` as neccessary.

```bash
export RESOURCE_GROUP='220600-cosmos-db'
export COSMOS_ACCOUNT_NAME='cosmos220600'

bash role-assign-create.sh
```

You can delete the role definition in [role-definition-rw.json](./role-definition-rw.json) using the following snippet.

```bash
ROLE_DEFINITION_ID=$(az cosmosdb sql role definition list \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --out tsv \
    --query '[?roleName == `MyReadWriteRole`] | [0].id')

echo "az cosmosdb sql role definition delete"
az cosmosdb sql role definition delete \
    --resource-group $RESOURCE_GROUP \
    --account-name $COSMOS_ACCOUNT_NAME \
    --id $ROLE_DEFINITION_ID
```
