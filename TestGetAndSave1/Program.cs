using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;
using MongoDB.Driver.Linq;
using MongoDB.Driver.Core;

namespace TestGetAndSave1
{
    class Program
    {
        static void Main(string[] args)
        {
            TestInsert();
            //var userName = "test";
            //var password = "password";

            //var client = new MongoClient("mongodb://test:P@ssw0rd@ds047782.mongolab.com:47782/testcrud");


            //var crudDb = client.GetDatabase("testcrud");

            //var itemCollection = crudDb.GetCollection<Aggregate>("aggregates");


            //itemCollection.

            //var na = new Aggregate {count = 1000, identifier = "0_MDT_A"};


            ////var server = client.GetDatabase("aggregates");

            //crudDb.GetCollection<Aggregate>("aggregates").InsertOneAsync(na).Wait();


            //var itemCollection1 = crudDb.GetCollection<Aggregate>("aggregates");






        }

        private async static void TestInsert()
        {
            var client = new MongoClient("mongodb://test1:test1@ds047782.mongolab.com:47782/testcrud");
            var database = client.GetDatabase("testcrud");
            var collection = database.GetCollection<BsonDocument>("aggregates");

            var json = collection.ToJson();

            //var document = new BsonDocument { { "identifier", "0_MDT_A" }, { "count", 200 } };
            //collection.InsertOneAsync(document).Wait();

            var docs = new List<BsonDocument>();

            for (var i = 0; i < 1000; i++)
            {
                var document = new BsonDocument { { "identifier", "0_MDT_A" }, { "count", 200 } };
                docs.Add(document);
            }

            collection.InsertManyAsync(docs).Wait();


            //collection.InsertManyAsync()

            //var filter = new BsonDocument("x", new BsonDocument("$gte", 100));
            //collection.Find(filter);

            //var filter = new BsonDocument();

            //var options = new FindOptions<BsonDocument>
            //{
            //    // Our cursor is a tailable cursor and informs the server to await
            //    CursorType = CursorType.TailableAwait
            //};


            //using (var cursor = await collection.FindAsync(filter))
            //{
            //    var results = await cursor.ToListAsync();



            //    var z = results;
            //    //{

            //    //    var y = x;
            //    //    var z = y;
            //    //});
            //    // This callback will get invoked with each new document found
            //    //await cursor.ForEachAsync(async doc =>
            //    //{
            //    //    // Set the last value we saw 
            //    //    var lastValue = document["insertDate"];

            //    //    // Write the document to the console.
            //    //    await Console.WriteLineAsync(doc.ToString());
            //    //});

            //}





        }
    }

    public class Aggregate
    {
        public int count { get; set; }

        public string identifier { get; set; }

    }
}
