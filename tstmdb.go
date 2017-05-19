package main

import (
	"fmt"
	"flag"
	"gopkg.in/mgo.v2"
	"log"
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

func main() {
	var writeToDB chan DateForTest

	flag.Parse()

	collect := initConnection(ip)
	defer collect.Database.Session.Close()

	go dataGenerator(writeToDB)






}

func insertToDB(db *mgo.Database, writeToDB <- chan typeCust) {
	c := db.C("Connections")
	for v := range writeToDB {
		WG.Done()
		err := c.Insert(v)
		if err != nil {
			CLog.PrintLog(true, err)
		}
	}
	return
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




}