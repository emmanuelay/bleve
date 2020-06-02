package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Pallinder/go-randomdata"
	age "github.com/bearbin/go-age"
	"github.com/blevesearch/bleve"
	"github.com/jedib0t/go-pretty/table"
)

// User contains a person
type User struct {
	ID           int       `json:"id"`
	Firstname    string    `json:"firstname"`
	Lastname     string    `json:"lastname"`
	Gender       string    `json:"gender"`
	BirthDate    time.Time `json:"birthdate"`
	Age          int       `json:"age"`
	CreatedAt    time.Time `json:"created_at"`
	LastOnlineAt time.Time `json:"last_online_at"`
}

func createUsers(numUsers int) []User {
	users := []User{}

	for i := 0; i < numUsers; i++ {
		strCreatedAt := randomdata.FullDateInRange("2019-04-01", "2020-03-02")
		createdAt, _ := time.Parse("Monday 2 Jan 2006", strCreatedAt)

		strLastOnlineAt := randomdata.FullDateInRange("2020-03-03", "2020-06-02")
		lastOnlineAt, _ := time.Parse("Monday 2 Jan 2006", strLastOnlineAt)

		strBirthdate := randomdata.FullDateInRange("1955-08-22", "2002-08-01")
		birthDate, _ := time.Parse("Monday 2 Jan 2006", strBirthdate)

		age := age.Age(birthDate)

		profile := randomdata.GenerateProfile(randomdata.Male | randomdata.Female | randomdata.RandomGender)

		user := User{
			ID:           i + 1000,
			Gender:       profile.Gender,
			Firstname:    profile.Name.First,
			Lastname:     profile.Name.Last,
			BirthDate:    birthDate,
			CreatedAt:    createdAt,
			LastOnlineAt: lastOnlineAt,
			Age:          age,
		}

		users = append(users, user)
	}

	return users
}

func seedIndex(index bleve.Index) {
	users := createUsers(100)

	batchSize := 10
	batchCount := 0

	fmt.Println("- Seeding index")
	batch := index.NewBatch()

	for _, user := range users {
		batch.Index(string(user.ID), user)
		batchCount = (batchCount + 1) % (batchSize + 1)

		if batchCount == batchSize {
			fmt.Println("- Seeding batch")
			err := index.Batch(batch)
			if err != nil {
				panic(err)
			}

			batch = index.NewBatch()
		}
	}

	if batchCount > 0 {
		fmt.Println("- Seeding last batch")
		err := index.Batch(batch)
		if err != nil {
			panic(err)
		}
	}
}

func search(searchString string, index bleve.Index) error {

	// Querystring query (matches on all fields)
	queryStringQuery := bleve.NewQueryStringQuery(searchString)

	// NumericRange query matches on ranges
	// minRange := 30.0
	// maxRange := 95.0
	// numericRangeQuery := bleve.NewNumericRangeQuery(&minRange, &maxRange)
	// numericRangeQuery.SetField("age")

	// Term query, matches on specific fields and specific values
	// termQuery := bleve.NewTermQuery("gender")
	// termQuery.Term = "male"
	// termQuery

	// Conjunction query, combines multiple queries
	//conjunctionQuery := bleve.NewConjunctionQuery(queryStringQuery)

	searchRequest := bleve.NewSearchRequest(queryStringQuery)
	searchRequest.IncludeLocations = false
	searchRequest.Fields = []string{"id", "firstname", "lastname", "age"}

	// Gender
	genderFacet := bleve.NewFacetRequest("gender", 2)
	genderFacet.Field = "gender"
	searchRequest.AddFacet("gender", genderFacet)

	// Age
	eighteen := 18.0
	twentyfive := 25.0
	fourtyfive := 45.0
	sixtyfour := 64.0
	ageFacet := bleve.NewFacetRequest("age", 5)
	ageFacet.AddNumericRange("teenager", nil, &eighteen)
	ageFacet.AddNumericRange("young-adult", &eighteen, &twentyfive)
	ageFacet.AddNumericRange("adult", &twentyfive, &fourtyfive)
	ageFacet.AddNumericRange("senior-adult", &fourtyfive, &sixtyfour)
	ageFacet.AddNumericRange("senior", &sixtyfour, nil)
	searchRequest.AddFacet("age_group", ageFacet)

	// Pagination
	searchRequest.From = 0 // Offset
	searchRequest.Size = 5 // Page size

	searchResult, err := index.Search(searchRequest)
	if err != nil {
		return err
	}

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"#", "First Name", "Last Name", "Age"})
	for _, item := range searchResult.Hits {
		t.AppendRows(
			[]table.Row{
				{
					item.Fields["id"].(float64),
					item.Fields["firstname"].(string),
					item.Fields["lastname"].(string),
					item.Fields["age"].(float64),
				},
			},
		)
	}
	t.Render()
	return nil
}

func main() {

	// Create new Index mapping
	mapping := bleve.NewIndexMapping()

	// Create in-memory store
	index, err := bleve.NewMemOnly(mapping)

	if err != nil {
		panic(err)
	}

	// Seed with dummy data
	seedIndex(index)

	// Create a STDIN reader
	reader := bufio.NewReader(os.Stdin)

	fmt.Println("Type searchstring and press ENTER (ex: '+ gender:female + age:>=45')")

	for {
		fmt.Print("-> ")

		text, _ := reader.ReadString('\n')
		// convert CRLF to LF
		text = strings.Replace(text, "\n", "", -1)

		if strings.Compare("exit", text) == 0 {
			fmt.Println("Goodbye!")
			return
		}

		err := search(text, index)
		if err != nil {
			fmt.Println("Error while searching!\n", err.Error())
		}
	}
}
