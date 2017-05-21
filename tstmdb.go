package main

import (
	"fmt"
	"flag"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"log"
	"math/rand"
)

const data  = "glhs'gljhs;dgjh'sdg'sdlfgb's;dfghpdodkpuhfd ihpritjhpifgjpf  ognborgnbirnogf;gkjfgnbogbogifnb ;j og or ognbofgbnf ofgjnbognbfgnblm ;ldfkgbfkglfgbn okgnlkdfglnf okfdng fd;gk fsd;ldkf ;dkg'fndf;lkgb;lfbn;lfkblsf b ;lkdfg;lkf;sl ; nf;dlkg;dfklgnnb;slfgbdmfgn;lkdjfhirjhkkfgl;nkdgfjhkfgnb;lfkk  ;lkgnblkgblk ;kgnbl'krnb r;onrkbkrnnlf ;ofgnb';fb;kfjgh;f ;jngfkngkbnfk k n'kkfflglhs'gljhs;dgjh'sdg'sdlfgb's;dfghpdodkpuhfd ihpritjhpifgjpf  ognborgnbirnogf;gkjfgnbogbogifnb ;j og or ognbofgbnf ofgjnbognbfgnblm ;ldfkgbfkglfgbn okgnlkdfglnf okfdng fd;gk fsd;ldkf ;dkg'fndf;lkgb;lfbn;lfkblsf b ;lkdfg;lkf;sl ; nf;dlkg;dfklgnnb;slfgbdmfgn;lkdjfhirjhkkfgl;nkdgfjhkfgnb;lfkk  ;lkgnblkgblk ;kgnbl'krnb r;onrkbkrnnlf ;ofgnb';fb;kfjgh;f ;jngfkngkbnfk k n'kkffl okngkkbs'kfb'dkfgfblf  okngkkbs'kfb'dkfgfblf glhs'gljhs;dgjh'sdg'sdlfgb's;dfghpdodkpuhfd ihpritjhpifgjpf  ognborgnbirnogf;gkjfgnbogbogifnb ;j og or ognbofgbnf ofgjnbognbfgnblm ;ldfkgbfkglfgbn okgnlkdfglnf okfdng fd;gk fsd;ldkf ;dkg'fndf;lkgb;lfbn;lfkblsf b ;lkdfg;lkf;sl ; nf;dlkg"

var (
	ip			string
	num, op			int
	remove, ins, del, prep	bool
)


func init() {
	flag.StringVar(&ip, "ip", "127.0.0.1", "ip-address (default : 127.0.0.1)")
	flag.IntVar(&num, "n", 1000, "number of requests (default: 1000)")
	flag.IntVar(&op, "o", 100, "number of operations (default : 100)")
	flag.BoolVar(&remove, "r", false, "remove database (default: false)")
	flag.BoolVar(&ins, "i", true, "insert the documents into database (default: true)")
	flag.BoolVar(&del, "d", true, "delete the documents database (default: true)")
	flag.BoolVar(&prep, "p", true, "preparing database (default: true)")
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
	fmt.Println("number of operations: ", op)
	fmt.Println("remove database: ", remove)
	fmt.Println("preparing database: ", prep)
	fmt.Println("insert the documents into database: ", ins)
	fmt.Println("delete the documents database: ", del)

	fmt.Println("\nConnecting to the database...")
	collect := MayCollect{initConnection(ip)}
	defer collect.Database.Session.Close()

	go dataGenerator(writeToDB)

	if prep {
		fmt.Println("Preparing the database...")
		fmt.Printf("The %d documents added.\n",
			collect.insertToCollect(writeToDB, num))
	}

	fmt.Println("Finding in the database...")
	fmt.Printf("The %d documents found.\n",
	collect.findInCollect(op))

	if del {
		fmt.Println("Deleting from the database...")
		fmt.Printf("The %d documents deleted.\n",
			collect.deleteFromCollect(op))
	}

	if ins {
		fmt.Println("Inserting to the database...")
		fmt.Printf("The %d documents inserted.\n",
			collect.insertToCollect(writeToDB, op))
	}

	if remove {
		fmt.Println("Removing the database...")
		collect.removeDataBase()
	}

	return
}

func (c *MayCollect)insertToCollect(writeToDB <- chan DateForTest, n int) int {

	i := 0
	k := 0
	for v := range writeToDB {
		i++
		if i > n { break }
		err := c.Insert(bson.M{"i": v.i, "text": v.s})
		if err != nil {
			log.Fatalln("Insert into DB: ", err)
		} else { k++ }
	}
	return k
}

func (c *MayCollect)removeDataBase() {
	err := c.Database.DropDatabase()
	if err != nil { log.Fatalln("Droping DB: ", err)}
	return
}

func (c *MayCollect)findInCollect(n int) int {
	var rez interface{}
	var err error

	k:=0
	for i :=0 ; i < n; i++ {
		err = c.Find(bson.M{"i": indexGenerator(num)}).One(&rez)
		if err == nil { k++ }
	}
	return k
}

func (c *MayCollect)deleteFromCollect(n int) int {
	k := 0
	q := c.Find(bson.M{})
	max, err := c.Count()
	if err != nil {log.Fatalln("Error detecting number of documents: ", err)}

	for i :=0 ; i < n; i++ {
		var result interface{}

		if max == 0 {
			fmt.Println("No more documents for deleting.")
			break
		}

		err = q.Skip(indexGenerator(max)).One(&result)
		if err != nil { fmt.Println("Delete from DB: ", err)}

		err = c.Remove(result)
		if err != nil { fmt.Println("Delete from DB: ", err)
		} else { k++ }
		max--
	}
	return k
}

func initConnection(ip string) *mgo.Collection {
	session, err := mgo.Dial(ip)
	if err != nil {
		log.Fatalln("Initialize connection with DB: ", err)
	}

	db := session.DB("DBForTest")
	c := db.C("Test")

	return c
}

func dataGenerator(writeToDB chan DateForTest) {
	var d DateForTest
	var i int64
	i = 1
	for {
		if i > int64(num) { i = 1 }
		d = DateForTest{i, data}
		writeToDB <- d
		i++
	}

	return
}

func indexGenerator(i int) int {
	return rand.Intn(i)
}




//func findInCollection(c *mgo.Collection, q interface{}, r interface{}) {
//	err := c.Find(q).All(r)
//	if err != nil {
//		log.Println(err)
//	}
//	return
//}