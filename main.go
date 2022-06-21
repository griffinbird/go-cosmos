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
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/google/uuid"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	databaseName := "database-v4"
	containerName := "customer"

	client, err := newClientFromEnviroment()
	if err != nil {
		return err
	}

	prompt := `Azure Cosmos DB Golang SDK Examples
-----------------------------------------
[a]   Query for single customer
[b]   Point read for single customer
[c]   List all product categories
[d]   Query products by category id
[e]   Update product category name
[f]   Query orders by customer id
[g]   Query for customer and all orders
[h]   Create new order and update order total
[i]   Delete order and update order total
[j]   Query top 10 customers
-------------------------------------------
[k]   Create databases and containers
[l]   Upload data to containers
[m]   Delete databases and containers
-------------------------------------------
[x]   Exit

> `

	// TODO:
	//  - clear the terminal screen after selection, press any key to return etc.
	//  - order map return json

out:
	for {
		fmt.Print("\n" + prompt)
		result := ""
		fmt.Scanln((&result))
		fmt.Printf("\nYour selection is: %v\n\n", result)

		switch result {
		case "a":
			pk := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
			databaseName := "database-v2"
			containerName := "customer"
			err := queryCustomer(client, containerName, databaseName, pk)
			if err != nil {
				return err
			}

		case "b":
			pk := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
			id := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
			databaseName := "database-v2"
			containerName := "customer"
			item, err := getCustomer(client, databaseName, containerName, pk, id)
			if err != nil {
				return err
			}
			b, err := json.MarshalIndent(item, "", "    ")
			if err != nil {
				return err
			}
			fmt.Printf("%s\n", b)

		case "c":
			databaseName := "database-v2"
			containerName := "productCategory"
			err := ListAllProductCategories(client, containerName, databaseName)
			if err != nil {
				return err
			}

		case "d":
			databaseName := "database-v4"
			containerName := "product"
			err := QueryProductsByCategoryId(client, databaseName, containerName)
			if err != nil {
				return err
			}

		case "e":
			// TODO - need to validate if the logic is correct
			//Change feed is a good option here
			databaseName := "database-v3"
			err := QueryProductsForCategory(client, databaseName, "product")
			if err != nil {
				return err
			}
			categoryId := "86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4"
			categoryName1 := "Accessories, Tires and Tubes"
			categoryName2 := "Accessories, Tires & Tubes"
			err = UpdateCategoryName(client, databaseName, categoryId, categoryName1)
			if err != nil {
				return err
			}
			err = QueryProductsForCategory(client, databaseName, "product")
			if err != nil {
				return err
			}
			err = UpdateCategoryName(client, databaseName, categoryId, categoryName2)
			if err != nil {
				return err
			}
			err = RevertProductCategory(client, databaseName, "productCategory")
			if err != nil {
				return err
			}

		case "f":
			databaseName := "database-v4"
			containerName := "customer"
			err := QuerySalesOrdersByCustomerId(client, containerName, databaseName)
			if err != nil {
				return err
			}

		case "g":
			databaseName := "database-v4"
			containerName := "customer"
			err := QueryCustomerAndSalesOrdersByCustomerId(client, containerName, databaseName)
			if err != nil {
				return err
			}

		case "h":
			b := `
			{
				"customerId": "54AB87A7-BDB9-4FAE-A668-AA9F43E26628",
				"details": [
					{
						"name": "Road-550-W Yellow, 42",
						"price": 1120.49,
						"quantity": 1,
						"sku": "BK-R64Y-42"
					},
					{
						"name": "Sport-100 Helmet, Blue",
						"price": 34.99,
						"quantity": 1,
						"sku": "HL-U509-B"
					}
				],
				"id": "",
				"orderDate": "2014-02-16T00:00:00",
				"shipDate": "2014-02-23T00:00:00",
				"type": "salesOrder"
			}			
			`

			item := map[string]interface{}{}
			err := json.Unmarshal([]byte(b), &item)
			if err != nil {
				return err
			}

			customerID := ""
			if item, ok := item["customerId"]; ok {
				if val, ok := item.(string); ok {
					customerID = val
				}
			}

			if customerID == "" {
				return errors.New("customerID is empty")
			}

			// check UUID option
			//customerID = uuid.New().String()
			//item["customerId"] = customerID
			orderID := uuid.New().String()
			item["id"] = orderID

			// TODO: make batch updates work
			/*
				err = CreateNewOrderAndUpdateCustomerOrderTotal(client, databaseName, containerName, customerID, item)
				if err != nil {
					return err
				}
			*/
			err = UpdateSalesOrderQty(client, databaseName, containerName, customerID, item)
			if err != nil {
				return err
			}

		case "i":
			orderId := "000C23D8-B8BC-432E-9213-6473DFDA2BC5"
			customerId := "54AB87A7-BDB9-4FAE-A668-AA9F43E26628"
			if err := DeleteCustomerOrder(client, databaseName, containerName, orderId, customerId); err != nil {
				return err
			}

		case "j":
			if err := GetTop10Customers(client, databaseName, containerName); err != nil {
				return err
			}

		case "k":
			if err := CreateDatabase(client); err != nil {
				return err
			}

		case "l":
			imports := []struct {
				URL       string
				PK        string
				Database  string
				Container string
			}{
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v2/customer",
					PK:        "id",
					Database:  "database-v2",
					Container: "customer",
				},
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v2/productCategory",
					PK:        "type",
					Database:  "database-v2",
					Container: "productCategory",
				},
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v3/product",
					PK:        "categoryId",
					Database:  "database-v3",
					Container: "product",
				},
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v3/productCategory",
					PK:        "type",
					Database:  "database-v3",
					Container: "productCategory",
				},
				{
					URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v4/customer",
					PK:        "customerId",
					Database:  "database-v4",
					Container: "customer",
				},
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
				// create the container
				err := createContainer(client, item.Database, item.Container, "/"+item.PK)
				if err != nil {
					return err
				}
				// ImportData
				log.Printf("importing Container %s from URL %s", item.Container, item.URL)
				err = ImportData(client, item.URL, item.PK, item.Database, item.Container)
				if err != nil {
					return err
				}
			}

		case "m":
			if err := DeleteDatabase(client); err != nil {
				return err
			}

		case "x":
			fmt.Println("exiting...")
			break out

		case "delete-item":
			pk := "category"
			id := "9a4f11d3-a60b-4baf-b8c2-bf83c1ff404b"
			_, err := deleteItem(client, "database-v4", "productMeta", pk, id)
			if err != nil {
				return err
			}

		case "test":
			tmp := struct {
				URL       string
				PK        string
				Database  string
				Container string
			}{
				URL:       "https://raw.githubusercontent.com/MicrosoftDocs/mslearn-cosmosdb-modules-central/main/data/fullset/database-v2/customer",
				PK:        "/id",
				Database:  "database-v2",
				Container: "customer",
			}

			err := createContainer(client, tmp.Database, tmp.Container, tmp.PK)
			if err != nil {
				return err
			}

		default:
			return errors.New("command doesn't exist. exiting")
		}
	}
	return nil
}

func newClientFromEnviroment() (*azcosmos.Client, error) {
	endpoint := os.Getenv("AZURE_COSMOS_ENDPOINT")
	if endpoint == "" {
		return nil, errors.New("AZURE_COSMOS_ENDPOINT could not be found")
	}

	key := os.Getenv("AZURE_COSMOS_KEY")

	if key != "" {

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

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, err
	}

	client, err := azcosmos.NewClient(endpoint, cred, nil)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func createContainer(client *azcosmos.Client, databaseName string, containerName string, partitionKey string) error {
	log.Printf("\nCreating container [%v] in database [%v]\n", containerName, databaseName)

	database, err := client.NewDatabase(databaseName)
	if err != nil {
		return err
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
			return err
		}
	} else {
		log.Printf("Container [%v] created. ActivityId %s\n", containerName, containerResp.ActivityID)
	}
	return nil
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

func queryCustomer(client *azcosmos.Client, containerName, databaseName, partitionKey string) error {
	//Querying for a single customer
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	fmt.Printf("\nQuerying customer id: [%v] in %v\\%v\n", pk, databaseName, containerName)

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

func getCustomer(client *azcosmos.Client, databaseName, containerName, partitionKey, id string) (map[string]interface{}, error) {
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	log.Printf("\nExecuting a point read against:\n PK: %v \n ID: %v\n\n", pk, id)

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
	log.Printf("\nItem %v read.\n Status:\t%d.\n ActivityId:\t%s.\n Request Units:\t%v\n", pk, itemResponse.RawResponse.StatusCode, itemResponse.ActivityID, itemResponse.RequestCharge)
	return item, nil
}

func ListAllProductCategories(client *azcosmos.Client, containerName, databaseName string) error {
	//Get all product categories
	query := "SELECT * FROM c WHERE c.type = 'category'"
	pk := azcosmos.NewPartitionKeyString("category")

	log.Printf("Print out all categories in %v\\%v\n", databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	queryPager := container.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
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

func QueryProductsByCategoryId(client *azcosmos.Client, databaseName, containerName string) error {
	//Category Name = Accessories, Tires and Tubes
	pk := azcosmos.NewPartitionKeyString("86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4")

	log.Printf("Retreiving all products by categoryId [%v] in [%v\\%v]", pk, databaseName, containerName)
	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}
	//Query for products by category id
	queryPager := container.NewQueryItemsPager("select * from c", pk, nil)
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

func RefreshProductCategory(client *azcosmos.Client, databaseName, containerName string) error {
	return nil
}

func QueryProductsForCategory(client *azcosmos.Client, databaseName, containerName string) error {
	categoryId := "86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4"
	pk := azcosmos.NewPartitionKeyString(categoryId)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}
	log.Printf("Printing how many products in categoryId [%v] in [%v\\%v]", pk, databaseName, containerName)

	query := "SELECT COUNT(1) AS ProductCount, c.categoryName " +
		"FROM c WHERE c.categoryId = '86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4'" +
		"GROUP BY c.categoryName"

	fmt.Printf("Print out category name and number of products in that category\n")

	queryPager := container.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
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

func UpdateCategoryName(client *azcosmos.Client, databaseName, categoryID, categoryName string) error {
	containerName := "productCategory"
	pkName := "category"
	item, err := pointRead(client, databaseName, containerName, pkName, categoryID)
	if err != nil {
		return err
	}

	// update value ie. fields id, type, value
	item["value"] = categoryName

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	pk := azcosmos.NewPartitionKeyString(pkName)
	res, err := container.UpsertItem(context.Background(), pk, b, nil)
	if err != nil {
		return err
	}
	_ = res

	return nil
}

func RevertProductCategory(client *azcosmos.Client, databaseName, containerName string) error {

	categoryId := "86F3CBAB-97A7-4D01-BABB-ADEFFFAED6B4" //Category Name = Accessories, Tires and Tubes
	pk := azcosmos.NewPartitionKeyString("category")

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	item := map[string]string{
		"id":    categoryId,
		"type":  "category",
		"value": "Accessories, Tires and Tubes",
	}

	marshalled, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		fmt.Printf("Error parsing JSON string - %s", err)
	}

	itemResponse, err := container.ReplaceItem(context.Background(), pk, categoryId, marshalled, &azcosmos.ItemOptions{EnableContentResponseOnWrite: true})
	if err != nil {
		fmt.Printf("Failed to replace item: %v", err)
	}
	fmt.Printf("Change category name back to the original (Accessories, Tires and Tubes)\n")
	log.Printf("Item [%v] read. Status %d. ActivityId %s. Consuming %v RU\n", categoryId, itemResponse.RawResponse.StatusCode, itemResponse.ActivityID, itemResponse.RequestCharge)

	id := categoryId
	item1, err := pointRead(client, "database-v3", "productCategory", "category", id)
	if err != nil {
		return err
	}
	b, err := json.MarshalIndent(item1, "", "    ")
	if err != nil {
		return err
	}
	fmt.Printf("%s\n", b)

	return nil
}

func QuerySalesOrdersByCustomerId(client *azcosmos.Client, containerName, databaseName string) error {
	pk := azcosmos.NewPartitionKeyString("FFD0DD37-1F0E-4E2E-8FAC-EAF45B0E9447")

	log.Printf("Print out all sales orders for customer with PK [%v] in %v\\%v\n", pk, databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)

	if err != nil {
		return err
	}
	query := "SELECT * from c WHERE c.type = 'salesOrder'"
	queryPager := container.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
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

	log.Printf("Print out customer record PK [%v] and all their sales orders in %v\\%v\n", pk, databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}
	query := "select * from c"
	queryPager := container.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
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

func CreateNewOrderAndUpdateCustomerOrderTotal(client *azcosmos.Client, databaseName, containerName, id string, item map[string]interface{}) error {
	log.Printf("Creating a new Order %v in %v\\%v\n", id, databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	b, err := json.Marshal(item)
	if err != nil {
		return err
	}
	pk := azcosmos.NewPartitionKeyString(id)
	ctx := context.Background()
	options := &azcosmos.ItemOptions{EnableContentResponseOnWrite: false}

	itemResponse, err := container.CreateItem(ctx, pk, b, options)
	if err != nil {
		var responseErr *azcore.ResponseError
		if errors.As(err, &responseErr) {
			if responseErr.ErrorCode == "Conflict" {
				log.Printf("Customer order already exists: %s\n", id)
			} else {
				return err
			}
		}
	} else {
		map1 := map[string]interface{}{}
		err = json.Unmarshal(b, &map1)
		if err != nil {
			return err
		}
		b, err = json.MarshalIndent(map1, "", "    ")
		if err != nil {
			return err
		}
		fmt.Printf("%s\n", b)
		log.Printf("Status %d. Item %v created. ActivityId %s. Consuming %v RU\n", itemResponse.RawResponse.StatusCode, id, itemResponse.ActivityID, itemResponse.RequestCharge)
	}
	return nil
}

func DeleteCustomerOrder(client *azcosmos.Client, databaseName, containerName, orderId, customerId string) error {
	pk := azcosmos.NewPartitionKeyString(customerId)

	log.Printf("Deleting customer order %v\n", customerId)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	itemResponse, err := container.DeleteItem(context.Background(), pk, orderId, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		return err
	}
	log.Printf("Customer Order [%v] deleted. Status %d. ActivityId %s. Consuming %v RU\n", customerId, itemResponse.RawResponse.StatusCode, itemResponse.ActivityID, itemResponse.RequestCharge)
	return err
}

func GetTop10Customers(client *azcosmos.Client, databaseName, containerName string) error {
	//Query to get our top 10 customers. Currently only for a single customer id.
	//TODO - Need to return all customers and pull out customer name and order qty and order by in code.
	customerId := "FFCAE1E9-7E8D-457B-8435-BB7992C6D8BF"
	pk := azcosmos.NewPartitionKeyString(customerId)

	log.Printf("Print out top 10 customers and number of orders in %v\\%v\n", databaseName, containerName)

	container, err := client.NewContainer(databaseName, containerName)

	if err != nil {
		return err
	}
	query := "SELECT TOP 10 c.firstName, c.lastName, c.salesOrderCount " +
		"FROM c WHERE c.type = 'customer' " +
		"ORDER BY c.salesOrderCount DESC"
	queryPager := container.NewQueryItemsPager(query, pk, &azcosmos.QueryOptions{PopulateIndexMetrics: true})
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
			// print Customer name: \t Orders:  (customer per line)
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

func pointRead(client *azcosmos.Client, databaseName, containerName, partitionKey, id string) (map[string]interface{}, error) {
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	log.Printf("Executing a point read against: PK [%v] ID [%v] in [%v\\%v]\n", pk, id, databaseName, containerName)

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

func ImportData(client *azcosmos.Client, url1, pk, databaseName, containerName string) error {

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

func UpdateSalesOrderQty(client *azcosmos.Client, databaseName, containerName, customerID string, item map[string]interface{}) error {
	log.Printf("Creating a new Sales Order for customer %v in %v\\%v\n", customerID, databaseName, containerName)
	partitionKey := azcosmos.NewPartitionKeyString(customerID)

	container, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return err
	}

	itemResponse, err := container.ReadItem(context.Background(), partitionKey, customerID, nil)
	if err != nil {
		var responseErr *azcore.ResponseError
		errors.As(err, &responseErr)
		return err
	}
	customer := map[string]interface{}{}
	itemID := customer["id"]
	customer["salesOrderCount"] = "3"
	strItemID := fmt.Sprintf("%v", itemID)
	//_ = strItemID

	err = json.Unmarshal(itemResponse.Value, &customer)
	if err != nil {
		return err
	}

	marshalledCustomer, err := json.Marshal(customer)
	if err != nil {
		return err
	}
	fmt.Printf("Customer:\n%s\n", marshalledCustomer)

	marshalledSalesOrder, err := json.Marshal(item)
	if err != nil {
		return err
	}
	fmt.Printf("Sales Order:\n%s\n", marshalledSalesOrder)

	batch := container.NewTransactionalBatch(partitionKey)

	// ...
	batch.CreateItem(marshalledSalesOrder, nil)
	batch.ReplaceItem(strItemID, marshalledCustomer, nil)
	// ...

	batchResponse, err := container.ExecuteTransactionalBatch(context.Background(), batch, nil)
	if err != nil {
		return err
	}

	if batchResponse.Success {
		// Transaction succeeded
		// We can inspect the individual operation results
		for index, operation := range batchResponse.OperationResults {
			fmt.Printf("Operation %v completed with status code %v consumed %v RU", index, operation.StatusCode, operation.RequestCharge)
			if index == 1 {
				// Read operation would have body available
				var itemResponseBody map[string]string
				err = json.Unmarshal(operation.ResourceBody, &itemResponseBody)
				if err != nil {
					return err
				}
			}
		}
	} else {
		// Transaction failed, look for the offending operation
		for index, operation := range batchResponse.OperationResults {
			if operation.StatusCode != http.StatusFailedDependency {
				log.Printf("Transaction failed due to operation %v which failed with status code %v", index, operation.StatusCode)
			}
		}
	}
	return nil
}
