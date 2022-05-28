package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	//"github.com/google/uuid"
)

func createDatabase(databaseName string) {
	fmt.Printf("Creating dateabase [%v]\n",databaseName)

	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	fmt.Println(os.ExpandEnv("Using Cosmos DB Endpoint $AZURE_COSMOS_ENDPOINT"))

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	databaseProperties := azcosmos.DatabaseProperties{ID: databaseName}
	options := &azcosmos.CreateDatabaseOptions{}
	databaseResponse, err := client.CreateDatabase(context.Background(), databaseProperties, options)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr.ErrorCode == "Conflict" {
			log.Printf("Database [%v] already exists\n", databaseName)
		} else {
			panic(responseErr)
		}
	} else {
		fmt.Printf("Database [%v] created. ActivityId %s\n", databaseName, databaseResponse.ActivityID)
	}
}

func createContainer(databaseName string, containerName string, partitionKey string) {
	fmt.Printf("\nCreating container [%v] in database [%v]\n", containerName,databaseName)

	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	fmt.Println(os.ExpandEnv("Using Cosmos DB Endpoint $AZURE_COSMOS_ENDPOINT"))

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	database, err := client.NewDatabase(databaseName)
	if err != nil {
		panic(err)
	}
	containerProperties := azcosmos.ContainerProperties{
		ID: containerName,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{partitionKey},
		},
	}
	throughput := azcosmos.NewManualThroughputProperties(400)
	containerResp, err := database.CreateContainer(context.Background(), containerProperties, &azcosmos.CreateContainerOptions{ThroughputProperties: &throughput})

	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr.ErrorCode == "Conflict" {
			log.Printf("Container [%v] already exists\n", containerName)
		} else {
			panic(responseErr)
		}
	} else {
		fmt.Printf("Container [%v] created. ActivityId %s\n", containerName, containerResp.ActivityID)
	}
}

func createItem(databaseName string, containerName string) {
	//Add an item to the container
	//uuid := uuid.New()
	fmt.Printf("\nCreating Item in %v\\%v\n",databaseName, containerName)
	pk := azcosmos.NewPartitionKeyString("pk19")

	item := map[string]string{
		"id":             "19",
		"value":          "6",
		"myPartitionKey": "pk19",
	}

	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	fmt.Println(os.ExpandEnv("Using Cosmos DB Endpoint $AZURE_COSMOS_ENDPOINT"))

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		panic(err)
	}

	marshalled, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		fmt.Printf("Error parsing JSON string - %s", err)
	}

	itemResponse, err := container.CreateItem(context.Background(), pk, marshalled, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		if responseErr.ErrorCode == "Conflict" {
			log.Printf("Item %v already exists\n", pk)
		} else {
			panic(responseErr)
		}
	} else {
		fmt.Println((string(marshalled)))
		fmt.Printf("Status %d. Item %v created. ActivityId %s. Consuming %v RU\n", itemResponse.RawResponse.StatusCode, pk, itemResponse.ActivityID, itemResponse.RequestCharge)
	}
}

func pointRead (databaseName string, containerName string) {
	pk := azcosmos.NewPartitionKeyString("pk15")
	id := "15"

	fmt.Printf("\nExecuting a point read against PK [%v] and ID [%v]\n", pk, id)

	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	fmt.Println(os.ExpandEnv("Using Cosmos DB Endpoint $AZURE_COSMOS_ENDPOINT"))

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	container, err := client.NewContainer(databaseName,containerName)
	if err != nil {
		panic(err)
	}

	itemResponse, err := container.ReadItem(context.Background(), pk, id, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		panic(responseErr)
	}

	var itemResponseBody map[string]interface{}
	err = json.Unmarshal([]byte(itemResponse.Value), &itemResponseBody)
	if err != nil {
		panic(err)
	}
	//fmt.Print(&itemResponseBody)

	fmt.Printf("Item [%v] read. Status %d. ActivityId %s. Consuming %v RU\n", pk, itemResponse.RawResponse.StatusCode,itemResponse.ActivityID, itemResponse.RequestCharge)
}

func queryItems(containerName string, databaseName string) {
	//Querying items
	pk := azcosmos.NewPartitionKeyString("partitionkey11")

	fmt.Printf("\nQuerying [%v] in databaase\\container %v\\%v\n",pk, databaseName, containerName)
	
	endpoint, ok := os.LookupEnv("AZURE_COSMOS_ENDPOINT")
	if !ok {
		panic("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key, ok := os.LookupEnv("AZURE_COSMOS_KEY")
	if !ok {
		panic("AZURE_COSMOS_KEY could not be found")
	}

	fmt.Println(os.ExpandEnv("Using Cosmos DB Endpoint $AZURE_COSMOS_ENDPOINT"))

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		panic(err)
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		panic(err)
	}

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		panic(err)
	}

	queryPager := container.NewQueryItemsPager("select * from myContainer c", pk, nil)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			panic(err)
		}
		for _, item := range queryResponse.Items {
			var itemResponseBody map[string]string
			json.Unmarshal((item), &itemResponseBody)
			if err != nil {
				fmt.Printf("Error parsing JSON string - %s", err)
			}
			//fmt.Print(&itemResponseBody)
		}
		fmt.Printf("Query page received with %v items. Status %d. ActivityId %s. Consuming %v RU\n", len(queryResponse.Items), queryResponse.RawResponse.StatusCode, queryResponse.ActivityID, queryResponse.RequestCharge)
	}
}

func main() {
	var databaseName = "myDatabase"
	var containerName = "myContainer"
	var partitionKey = "/myPartitionKey"

	createDatabase(databaseName)
	createContainer(databaseName, containerName, partitionKey)
	createItem(databaseName, containerName)
	queryItems(containerName, databaseName)
	pointRead(databaseName, containerName)
}
