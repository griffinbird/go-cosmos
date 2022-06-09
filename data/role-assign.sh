resourceGroupName=<'cdb_resource_group'>
accountName=<'cdb_account_name'>

AD_OBJECT_ID="$(az ad signed-in-user show --out tsv --query objectId)"
roleDefinitionId=<'roleDefinitionID'>

az cosmosdb sql role assignment create \
    --account-name $accountName \
    --resource-group $resourceGroupName \
    --scope "/" --principal-id $AD_OBJECT_ID \
    --role-definition-id $roleDefinitionId