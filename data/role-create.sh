resourceGroupName=<'cdb_resource_group'>
accountName=<'cdb_account_name'>

az cosmosdb sql role definition create \
    --account-name $accountName \
    --resource-group $resourceGroupName \
    --body @role-definition-rw.json


