package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
	//"github.com/Azure/azure-sdk-for-go/sdk/internal/uuid"
	//"github.com/google/uuid"
)

func newClientFromEnviroment() (*azcosmos.Client, error) {
	endpoint := os.Getenv("AZURE_COSMOS_ENDPOINT")
	if endpoint == "" {
		return nil, errors.New("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key := os.Getenv("AZURE_COSMOS_KEY")
	if key == "" {
		return nil, errors.New("AZURE_COSMOS_KEY could not be found")
	}

	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		return nil, err
	}

	client, err := azcosmos.NewClientWithKey(endpoint, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func createContainer(databaseName string, containerName string, partitionKey string) {
	fmt.Printf("\nCreating container [%v] in database [%v]\n", containerName, databaseName)

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
	//Add an item to the container7
	id := uuid.New()
	fmt.Printf("\nCreating Item in %v\\%v\n", databaseName, containerName)
	pk := azcosmos.NewPartitionKeyString("category")
	item := map[string]string{
		"id":  			id.String(),
		"name":			"Bikes, BMX",
		"type": 		"category",
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

func pointRead(client *azcosmos.Client, databaseName, containerName, partitionKey, id string) (map[string]interface{}, error) {
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	log.Printf("Executing a point read against PK [%v] and ID [%v]\n", pk, id)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, err
	}

	itemResponse, err := container.ReadItem(context.Background(), pk, id, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		return nil, err
	}

	item := map[string]interface{}{}
	err = json.Unmarshal(itemResponse.Value, &item)
	if err != nil {
		return nil, err
	}
	log.Printf("Item [%v] read. Status %d. ActivityId %s. Consuming %v RU\n", pk, itemResponse.RawResponse.StatusCode, itemResponse.ActivityID, itemResponse.RequestCharge)
	return item, nil
}

func deleteItem(client *azcosmos.Client, databaseName, containerName, partitionKey, id string) (map[string]interface{}, error) {
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	log.Printf("Executing a delete against PK [%v] and ID [%v]\n", pk, id)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, err
	}
	
	itemResponse, err := container.DeleteItem(context.Background(), pk, id, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		return nil, err
	}
	log.Printf("Item [%v] deleted. Status %d. ActivityId %s. Consuming %v RU\n", id, itemResponse.RawResponse.StatusCode, itemResponse.ActivityID, itemResponse.RequestCharge)
	return nil, err
}

func queryItems(client *azcosmos.Client, containerName, databaseName string) error {
	//Querying items
	pk := azcosmos.NewPartitionKeyString("FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF")

	fmt.Printf("\nQuerying [%v] in %v\\%v\n", pk, databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	queryPager := container.NewQueryItemsPager("select * from customer c", pk, nil)

	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, item := range queryResponse.Items {
			map1 := map[string]interface{}{}
			err := json.Unmarshal(item, &map1)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(map1, "", "    ")
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
		log.Printf("Query page received with %d items. Status %d. ActivityId %s. Consuming %v RU\n", len(queryResponse.Items), queryResponse.RawResponse.StatusCode, queryResponse.ActivityID, queryResponse.RequestCharge)
	}
	return nil
}

func QueryCustomerAndSalesOrdersByCustomerId(client *azcosmos.Client, containerName, databaseName string) error {
	pk := azcosmos.NewPartitionKeyString("FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF")

	log.Printf("Retreiving all sales orders for customer with PK [%v]\n", pk)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	queryPager := container.NewQueryItemsPager("select * from c where c.type = 'salesOrder' and c.customerId ='FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF'", pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, item := range queryResponse.Items {
			map1 := map[string]interface{}{}
			err := json.Unmarshal(item, &map1)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(map1, "", "    ")
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
		log.Printf("Query page received with %d items. Status %d. ActivityId %s. Consuming %v RU\n", len(queryResponse.Items), queryResponse.RawResponse.StatusCode, queryResponse.ActivityID, queryResponse.RequestCharge)
	}
	return nil
}

func QueryProductsByCategoryId(client *azcosmos.Client, containerName, databaseName string) error {
	//Category Name = Components, Headsets
	pk := azcosmos.NewPartitionKeyString("AB952F9F-5ABA-4251-BC2D-AFF8DF412A4A")

	log.Printf("Retreiving all products by categoryId [%v]\n", pk)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}
	//Query for products by category id
	queryPager := container.NewQueryItemsPager("select * from c",pk, nil)
	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(context.Background())
		if err != nil {
			return err
		}
		for _, item := range queryResponse.Items {
			map1 := map[string]interface{}{}
			err := json.Unmarshal(item, &map1)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(map1, "", "    ")
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)
		}
		log.Printf("Query page received with %d items. Status %d. ActivityId %s. Consuming %v RU\n", len(queryResponse.Items), queryResponse.RawResponse.StatusCode, queryResponse.ActivityID, queryResponse.RequestCharge)
	}
	return nil
}

func DeleteDatabase(client *azcosmos.Client) error {
	var schemaVersionStart int = 1
	var schemaVersionEnd int = 4
	var schemaVersion = 0
	if !(schemaVersion == 0) {
		schemaVersionStart = schemaVersion
		schemaVersionEnd = schemaVersion
	} else {
		schemaVersionStart = 1
		schemaVersionEnd = 4
	}
	for schemaVersionCounter := schemaVersionStart; schemaVersionCounter <= schemaVersionEnd; schemaVersionCounter++ {
		err := DeleteDatabaseAndContainers(client, "database-v"+strconv.Itoa(schemaVersionCounter))
		if err != nil {
			return err
		}
	}
	return nil
}

func DeleteDatabaseAndContainers(client *azcosmos.Client, databaseName string) error {
	db, _ := client.NewDatabase(databaseName)
	//resp, err = db.Read(context.TODO(), nil)

	var response string
	fmt.Printf("Are you sure you want to delete [%v](Y/N) : ", databaseName)
	fmt.Scanln((&response))
	if strings.ContainsRune(response, 'y') || strings.ContainsRune(response, 'Y') {
		resp, err := db.Delete(context.TODO(), nil)
		_ = resp
		if err != nil {
			var responseErr *azcore.ResponseError
			errors.As(err, &responseErr)
			if responseErr.ErrorCode == "Conflict" {
				log.Printf("Database [%v] already exists\n", databaseName)
			} else {
				return err
			}
		} else {
			fmt.Printf("Database [%v] deleted. ActivityId %s\n", databaseName, resp.ActivityID)
		}
	}
	return nil
}

func CreateDatabase(client *azcosmos.Client) error {
	schemaVersionStart := 1
	schemaVersionEnd := 4
	schemaVersion := 0
	if !(schemaVersion == 0) {
		schemaVersionStart = schemaVersion
		schemaVersionEnd = schemaVersion
	} else {
		schemaVersionStart = 1
		schemaVersionEnd = 4
	}
	for schemaVersionCounter := schemaVersionStart; schemaVersionCounter <= schemaVersionEnd; schemaVersionCounter++ {
		fmt.Printf("Create started for schema %v\n", schemaVersionCounter)
		err := CreateDatabaseAndContainers(client, "database-v"+strconv.Itoa(schemaVersionCounter), schemaVersionCounter)
		if err != nil {
			return err
		}
	}
	return nil
}

func CreateDatabaseAndContainers(client *azcosmos.Client, databaseName string, schema int) error {
	if schema >= 1 && schema <= 4 {
		//throughput := azcosmos.NewManualThroughputProperties(400)
		databaseProperties := azcosmos.DatabaseProperties{ID: databaseName}
		databaseOptions := &azcosmos.CreateDatabaseOptions{}
		databaseResp, err := client.CreateDatabase(context.Background(), databaseProperties, databaseOptions)
		if err != nil {
			var responseErr *azcore.ResponseError
			errors.As(err, &responseErr)
			if responseErr.ErrorCode == "Conflict" {
				log.Printf("Database [%v] already exists\n", databaseName)
			} else {
				return err
			}
		} else {
			fmt.Printf("Database [%v] created. ActivityId %s\n", databaseName, databaseResp.ActivityID)
		}
	}
	return nil
}

func testImport(client *azcosmos.Client, url1, pk, databaseName, containerName string) error {

	res, err := http.Get(url1)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	b, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	items := []map[string]interface{}{}
	err = json.Unmarshal(b, &items)
	if err != nil {
		return err
	}
	db, err := client.NewDatabase(databaseName)
	if err != nil {
		return err
	}
	container, err := db.NewContainer(containerName)
	if err != nil {
		return err
	}

	ctx := context.Background()

	ruSum := 0.0
	start := time.Now()

	for _, item := range items {
		// pretty print as we insert
		b, err := json.MarshalIndent(item, "", "    ")
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)

		// insert the item
		id, ok := item[pk]
		if !ok {
			return fmt.Errorf("item does not have member %s", pk)
		}
		val, ok := id.(string)
		if !ok {
			return fmt.Errorf("item member %s should be a string", pk)
		}
		pk := azcosmos.NewPartitionKeyString(val)
		res, err := container.CreateItem(ctx, pk, b, nil)
		if err != nil {
			return err
		}
		ruSum = ruSum + float64(res.RequestCharge)
	}

	elapsed := time.Since(start)
	log.Printf("Total RUs consumed: %f in %f seconds\n", ruSum, elapsed.Seconds())

	return nil
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseName := "database-v4"
	containerName := "productMeta"
	partitionKey := "type"

	client, err := newClientFromEnviroment()
	if err != nil {
		return err
	}

	prompt := `Azure Cosmos DB Golang SDK Examples
-----------------------------------------
[a]   Query for single customer
[b]   Point read for single customer
[c]   Create item
[d]   List all product categories
[e]   Query products by category id
[f]   Update product category name
[g]   Query orders by customer id
[h]   Query for customer and all orders
[i]   Create new order and update order total
[j]   Delete order and update order total
[k]   Query top 10 customers
-------------------------------------------
[l]   Create databases and containers
[m]   Upload data to containers
[o]   Delete databases and containers
-------------------------------------------
[x]   Exit

> `

out:
	for {
		fmt.Print(prompt)
		result := ""
		fmt.Scanln((&result))
		fmt.Printf("\nYour selection is: %v\n", result)

		switch result {
		case "a":
			err := queryItems(client, "customer", databaseName)
			if err != nil {
				return err
			}
		case "b":
			pk := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
			id := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
			item, err := pointRead(client, databaseName, "customer", pk, id)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(item, "", "    ")
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)

		case "c":
			createItem(databaseName, containerName)
		case "g":
			err := QueryCustomerAndSalesOrdersByCustomerId(client, databaseName, containerName)
			if err != nil {
				return err
			}
		case "e":
			err := QueryProductsByCategoryId(client, databaseName, "product")
			if err != nil {
				return err
			}
		case "l":
			if err := CreateDatabase(client); err != nil {
				return err
			}
			createContainer(databaseName, containerName, partitionKey)
		case "o":
			if err := DeleteDatabase(client); err != nil {
				return err
			}
		case "x":
			fmt.Println("exiting...")
			break out
		case "delete-item":
			pk := "category"
			id := "9a4f11d3-a60b-4baf-b8c2-bf83c1ff404b"
			_ , err := deleteItem(client, "database-v4", "productMeta", pk, id)
			if err != nil {
				return err
			}
		case "test":

			url1 := "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v4/customer"
			pk := "id"
			err := testImport(client, url1, pk, "myDatabase", "container1")
			if err != nil {
				return err
			}

		case "test-bulk":
			imports := []struct {
				URL       string
				PK        string
				Database  string
				Container string
			}{
				/* {
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v4/customer",
					PK:        "customerId",
					Database:  "database-v4",
					Container: "customer",
				}, */
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v4/product",
					PK:        "categoryId",
					Database:  "database-v4",
					Container: "product",
				},
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v4/productMeta",
					PK:        "type",
					Database:  "database-v4",
					Container: "productMeta",
				},
			}

			for _, item := range imports {
				// deleteContainer
				// createContainer + handle errors...
				log.Printf("importing Container %s from URL %s", item.Container, item.URL)
				err := testImport(client, item.URL, item.PK, item.Database, item.Container)
				if err != nil {
					return err
				}
			}

		default:
			return errors.New("command doesn't exist. exiting")
		}
	}
	return nil
}
