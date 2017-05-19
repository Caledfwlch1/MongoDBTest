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
)


func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "ip-address")
	flag.IntVar(&num, "n", 1000, "number of requests")
}

type DateForTest struct {
	i	int64
	s	string
}

type MayCollect struct {
	mgo.Collection
}

func main() {
	writeToDB := make(chan DateForTest, 1000)

	flag.Parse()

	collect := MayCollect(initConnection(ip))
	defer collect.Database.Session.Close()

	go dataGenerator(writeToDB)

	fmt.Println("Preparing the database...")
	collect.insertToCollect(writeToDB)

	fmt.Println("Finding in the database...")
	collect.findInCollect()

	fmt.Println("Deleting from the database...")
	collect.deleteFromCollect()





}

func (c *MayCollect)insertToCollect(writeToDB <- chan DateForTest) {

	i := 1
	for v := range writeToDB {
		if i > num { break }
		err := c.Insert(v)
		if err != nil {
			log.Fatalln(err)
		}
		i++
	}
	return
}

func (c *MayCollect)deleteFromCollect() {
	var err error

	del := num/10
	for i :=1 ; i < del; i++ {
		err = c.Remove(bson.M{"i":fmt.Sprint(indexGenerator(num))})
		if err != nil { log.Fatalln(err)}
	}
}

func (c *MayCollect)findInCollect() {
	var rez DateForTest

	del := num/10
	for i :=1 ; i < del; i++ {
		c.Find(bson.M{"i":fmt.Sprint(indexGenerator(num))}).One(&rez)

	}
}

func findInCollection(c *mgo.Collection, q interface{}, r interface{}) {
	err := c.Find(q).All(r)
	if err != nil {
		CLog.PrintLog(true, err)
	}
	return
}

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