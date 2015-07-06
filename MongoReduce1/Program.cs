//using System;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Driver;

namespace MongoReduce1
{

    //http://www.codeproject.com/Articles/524602/Beginners-guide-to-using-MongoDB-and-the-offic

    /*
    var person = new ExpandoObject();
person.FirstName = "Jane";
person.Age = 12;
person.PetNames = new List<dynamic> { "Sherlock", "Watson" }
await db.GetCollection<dynamic>("people").InsertOneAsync(person);
    */
    internal class Program
    {
        private static void Main(string[] args)
        {

            //var fact = new MongoDB.Driver.Core.Servers.ServerFactory();
            var client = new MongoClient("mongodb://test:test@ds041188.mongolab.com:41188/testreduce1");
            //MongoServer server = client.GetServer();
            var database = client.GetDatabase("testreduce1");

            var collection = database.GetCollection<Movie>("movies");


            //AddMovies(collection);

            Reduce(database);

        }

        public static void DemoMapReduce(IMongoCollection<Movie> collection)
        {
            //The match function has to be written in JavaScript 
            //the map method outputs a key value pair for each document
            //in this case the key is the Lastname property value and the value is the Age property value
            var map =
                new BsonJavaScript(
                    @"function() 
                                            {
                                             //Associate each LastName property with the Age value
                                               emit(this.Lastname,this.Age);
                                             }");
            //The MapReduce method  uses the output from the Map method
            //   to produce a list of ages for each unique LastName value.
            //  Then each key and its associated list of values is presented to the Reduce method in turn.
            var reduce =
                new BsonJavaScript(
                    @"function(lastName,ages) 
                                             {
                                                 return Array.sum(ages);
                                              }");
            //The Reduce method returns the Lastname as the key and the sum of the ages as the value
            //The beauty of this technique is that data can be processed in batches
            //The output of one batch is combined with that of another and fed back through the Reducer
            //This is repeated until the output is reduced to the number of unique keys.
            // The results are output to a new collection named ResultsCollection on the server. 
            // This saves on the use of computer memory and enables the results to be queried effectively by using indexes. 

            //MapReduceOutputOptio
            IEnumerable<BsonDocument> resultAsBsonDocumentCollection =
                collection.MapReduceAsync(map, reduce,new MapReduceOptions<Movie,Movie>(MapReduceOutput.Replace("Reduce"))).
                    GetResults();
            Console.WriteLine("The total age for every member of each family  is ....");
            var reduction =
                resultAsBsonDocumentCollection.Select(
                    doc => new { family = doc["_id"].AsString, age = (int)doc["value"].AsDouble });
            foreach (var anon in reduction)
            {
                Console.WriteLine("{0} Family Total Age {1}", anon.family, anon.age);
            }
        }

        private static void Reduce(IMongoDatabase database)
        {
            string map = @"
    function() {
        var movie = this;
        emit(movie.Category, { count: 1, totalMinutes: movie.Minutes });
    }";

            string reduce = @"        
    function(key, values) {
        var result = {count: 0, totalMinutes: 0 };

        values.forEach(function(value){               
            result.count += value.count;
            result.totalMinutes += value.totalMinutes;
        });

        return result;
    }";


            string finalize = @"
    function(key, value){
      
      value.average = value.totalMinutes / value.count;
      return value;

    }";

            //var collection = database.GetCollection<Movie>("movies");
            //var options = new MapReduceOptions //new MapReduceOptionsBuilder();
            //options.SetFinalize(finalize);
            //options.SetOutput(MapReduceOutput.Inline);
            //var results = collection.MapReduce(map, reduce, options);

            //foreach (var result in results.GetResults())
            //{
            //    Console.WriteLine(result.ToJson());
            //}


        }

        private static void AddMovies(IMongoCollection<Movie> collection)
        {
            var movies = new List<Movie>
            {
                new Movie
                {
                    Title = "The Perfect Developer",
                    Category = "SciFi",
                    Minutes = 118
                },
                new Movie
                {
                    Title = "Lost In Frankfurt am Main",
                    Category = "Horror",
                    Minutes = 122
                },
                new Movie
                {
                    Title = "The Infinite Standup",
                    Category = "Horror",
                    Minutes = 341
                }
            };
            //collection.InsertBatch(movies);
            collection.InsertManyAsync(movies).Wait();
        }
    }

    public class Movie
    {
        public string Title { get; set; }
        public string Category { get; set; }
        public int Minutes { get; set; }

    }



}
