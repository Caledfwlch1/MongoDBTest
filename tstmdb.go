package main

import (
	"fmt"
	"flag"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math/rand"
)

var (
	ip	string
	num	int
	ins	int
	remove	bool
)


func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "ip-address (default : 127.0.0.1)")
	flag.IntVar(&num, "n", 1000, "number of requests (default: 1000)")
	flag.IntVar(&ins, "i", 100, "number of inserts (default : 100)")
	flag.BoolVar(&remove, "r", false, "remove database (default: false)")
}

type DateForTest struct {
	i	int64
	s	string
}

type MayCollect struct {
	*mgo.Collection
}

func main() {
	writeToDB := make(chan DateForTest, 1000)

	flag.Parse()

	fmt.Println("Current settings:")
	fmt.Println("ip-address: ", ip)
	fmt.Println("number of requests: ", num)
	fmt.Println("number of inserts: ", ins)
	fmt.Println("remove database: ", remove)

	fmt.Println("\nConnecting to the database...")
	collect := MayCollect{initConnection(ip)}
	defer collect.Database.Session.Close()

	go dataGenerator(writeToDB)

	fmt.Println("Preparing the database...")
	collect.insertToCollect(writeToDB, num)

	fmt.Println("Finding in the database...")
	collect.findInCollect(ins)

	fmt.Println("Deleting from the database...")
	collect.deleteFromCollect(ins)

	fmt.Println("Inserting to the database...")
	collect.insertToCollect(writeToDB, ins)

	if remove {
		fmt.Println("Removing the database...")
		collect.removeDataBase()
	}

	return
}

func (c *MayCollect)insertToCollect(writeToDB <- chan DateForTest, n int) {

	i := 1
	for v := range writeToDB {
		if i > n { break }
		err := c.Insert(v)
		if err != nil {
			log.Fatalln(err)
		}
		i++
	}
	return
}

func (c *MayCollect)removeDataBase() {
	err := c.Database.DropDatabase()
	if err != nil { log.Fatalln(err)}
	return
}

func (c *MayCollect)findInCollect(n int) {
	var rez DateForTest

	for i :=1 ; i < n; i++ {
		c.Find(bson.M{"i":fmt.Sprint(indexGenerator(num))}).One(&rez)

	}
}

func (c *MayCollect)deleteFromCollect(n int) {
	var err error

	for i :=1 ; i < n; i++ {
		err = c.Remove(bson.M{"i":fmt.Sprint(indexGenerator(num))})
		if err != nil { log.Fatalln(err)}
	}
}

//func findInCollection(c *mgo.Collection, q interface{}, r interface{}) {
//	err := c.Find(q).All(r)
//	if err != nil {
//		log.Println(err)
//	}
//	return
//}

func initConnection(ip string) (c *mgo.Collection) {
	session, err := mgo.Dial(ip)
	if err != nil {
		log.Fatalln(err)
	}

	db := session.DB("DBForTest")
	c = db.C("Test")

	return c
}

func dataGenerator(writeToDB chan DateForTest) {
	var d DateForTest
	var i int64
	i = 1
	for {
		if i > int64(num) { i = 1 }
		d = DateForTest{i, "sdfsdfhsdgsfg"}
		writeToDB <- d
		i++
	}

	return
}

func indexGenerator(i int) (j int) {
	rand.Seed(int64(i))
	return rand.Intn(i)
}