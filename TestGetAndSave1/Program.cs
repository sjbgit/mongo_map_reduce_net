using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;
using System.Linq;
using MongoDB.Driver.Core.Operations;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;
using MongoDB.Driver.Linq;
using MongoDB.Driver.Core;

namespace TestGetAndSave1
{
    class Program
    {
        static void Main(string[] args)
        {
            TestInsertPulseResponsePowerSetRadomRealWorld();

            //THIS WORKS
            //TestInsertPulseResponsePowerSet();

            //THIS WORKS
            //TestInsert();


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


        private static void TestInsertPulseResponsePowerSetRadomRealWorld()
        {
            var client = new MongoClient("mongodb://test1:test1@ds047782.mongolab.com:47782/testcrud");
            var database = client.GetDatabase("testcrud");
            var collection = database.GetCollection<BsonDocument>("pulseresponse_0");

            //var interval = 0;

            var options = new List<string> { "A", "B", "C", "D", "E" };
            
            var gender = new List<string> { "Male", "Female"};
            var political = new List<string> {"Democrat", "Republican", "Independent"};
            var education = new List<string> { "None", "HighSchool", "SomeCollege", "Bachelors", "Masters" };
            var age = new List<string> { "18to20", "21to30", "31to40", "41to50", "51+" };


            var docs = new List<BsonDocument>();

            for (var i = 0; i < 100000; i++)
            {

                var demos = new List<string>
                {
                    gender.RandomFirst(),
                    political.RandomFirst(),
                    education.RandomFirst(),
                    age.RandomFirst(),
                };

                var optionId = options.RandomFirst();

                var interval = Enumerable.Range(0, 1079).ToList().RandomFirst();

                var powerSet = demos.GetPowerSet().ToList();
                var delimtedPowerSet = powerSet.Select(x => String.Join("|", x.ToArray()));
                var preAndPostFixedPowerset = delimtedPowerSet.Select(x => interval + "_" + x + "_" + optionId).ToList();

                var documents =
                preAndPostFixedPowerset.Select(x => new BsonDocument { { "identifier", x }, { "count", 200 } })
                    .ToList(); //new BsonDocument { { "identifier", "7_MDT_A_" }, { "count", 200 } };

                //var document = new BsonDocument { { "identifier", "7_MDT_A_" + i.ToString().Reverse().First() }, { "count", 200 } };
                //var document = new BsonDocument { { "identifier", "7_MDT_A_" }, { "count", 200 } };
                //docs.Add(document);

                docs.AddRange(documents);

                if (docs.Count >= 1000)
                {
                    collection.InsertManyAsync(docs).Wait();
                    docs.Clear();
                }


            }


            collection.InsertManyAsync(docs).Wait();


            //var demos = (new List<int> { 99, 88 }).OrderBy(x => x).ToList(); //ascending

            //var powerSet = demos.GetPowerSet().ToList();

            //var joinedDemos = String.Join("|", demos.ToArray());//pipedelimited


            //var delimtedPowerSet = powerSet.Select(x => String.Join("|", x.ToArray()));


            //var id = interval + "_" + joinedDemos + "_" + optionId;


            //var preAndPostFixedPowerset = delimtedPowerSet.Select(x => interval + "_" + x + "_" + optionId).ToList();

            //do in-memory pre-aggregation


            //var documents =
            //    preAndPostFixedPowerset.Select(x => new BsonDocument { { "identifier", x }, { "count", 200 } })
            //        .ToList(); //new BsonDocument { { "identifier", "7_MDT_A_" }, { "count", 200 } };


            //collection.InsertManyAsync(documents).Wait();


        }


        private static void TestMapReduce()
        {

            const string map = @"function () {
                    emit(this.identifier, this.count);
                }";


            const string reduce = @"function (gender, count) {
                    return Array.sum(count);
                }";


            var client = new MongoClient("mongodb://test1:test1@ds047782.mongolab.com:47782/testcrud");
            var database = client.GetDatabase("testcrud");
            var collection = database.GetCollection<BsonDocument>("aggregates");

            var bsonMap = new BsonJavaScript(map);

            var bsonReduce = new BsonJavaScript(reduce);

            //collection.MapReduceAsync(bsonMap, bsonReduce, )

            var x = new MapReduceOutputToCollectionOperation(new CollectionNamespace("testcrud", "aggregates"),
                new CollectionNamespace("testcrud", "reduced2"), bsonMap, bsonReduce, new MessageEncoderSettings());

            //x.ExecuteAsync();
        }


        private static void TestInsertPulseResponsePowerSet()
        {
            var client = new MongoClient("mongodb://test1:test1@ds047782.mongolab.com:47782/testcrud");
            var database = client.GetDatabase("testcrud");
            var collection = database.GetCollection<BsonDocument>("pulseresponse_0");

            var interval = 0;

            var optionId = 2000;

            var demos = (new List<int> {99, 88}).OrderBy(x => x).ToList(); //ascending

            var powerSet = demos.GetPowerSet().ToList();

            var joinedDemos = String.Join("|", demos.ToArray());//pipedelimited


            var delimtedPowerSet = powerSet.Select(x => String.Join("|", x.ToArray()));


            var id = interval + "_" + joinedDemos + "_" + optionId;


            var preAndPostFixedPowerset = delimtedPowerSet.Select(x => interval + "_" + x + "_" + optionId).ToList();

            //do in-memory pre-aggregation


            var documents =
                preAndPostFixedPowerset.Select(x => new BsonDocument {{"identifier", x}, {"count", 200}})
                    .ToList(); //new BsonDocument { { "identifier", "7_MDT_A_" }, { "count", 200 } };


            collection.InsertManyAsync(documents).Wait();


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

            for (var i = 0; i < 2; i++)
            {
                //var document = new BsonDocument { { "identifier", "7_MDT_A_" + i.ToString().Reverse().First() }, { "count", 200 } };
                var document = new BsonDocument { { "identifier", "7_MDT_A_" }, { "count", 200 } };
                docs.Add(document);
            }

            //var options = new MapReduceOptions<Aggregate, Aggregate>();
            //options

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

    public static class ListExtensions
    {
        public static T Shift<T>(this List<T> list)
        {
            var item = list[0];
            list.RemoveAt(0);
            return item;
        }

        public static IEnumerable<IEnumerable<T>> GetPowerSet<T>(this List<T> list, bool excludeEmptySet = true)
        {
            var result = from m in Enumerable.Range(0, 1 << list.Count)
                         select
                             from i in Enumerable.Range(0, list.Count)
                             where (m & (1 << i)) != 0
                             select list[i];

            return excludeEmptySet ? result.ToList().Skip(1) : result;
        }



        public static T RandomFirst<T>(this List<T> list)
        {
            return list.OrderBy(x => Guid.NewGuid()).FirstOrDefault();
        }
    }
}
