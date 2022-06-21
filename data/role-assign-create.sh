[[ -z "${RESOURCE_GROUP:-}" ]] && RESOURCE_GROUP='220600-cosmos-db'
[[ -z "${COSMOS_ACCOUNT_NAME:-}" ]] && COSMOS_ACCOUNT_NAME='cosmos220600'

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