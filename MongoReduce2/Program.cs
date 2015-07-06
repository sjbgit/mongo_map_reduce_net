using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using MongoDB.Bson;
using MongoDB.Bson.IO;
using MongoDB.Driver;
using MongoDB.Driver.Core.Operations;
using MongoDB.Driver.Core.WireProtocol.Messages.Encoders;

namespace MongoReduce2
{
    class Program
    {
        static void Main(string[] args)
        {
            Mr1();
        }


        private static void Mr1()
        {

            var map = @"
    function() {
        var movie = this;
        emit(movie.Category, { count: 1, totalMinutes: movie.Minutes });
    }";

            var reduce = @"        
    function(key, values) {
        var result = {count: 0, totalMinutes: 0 };

        values.forEach(function(value){               
            result.count += value.count;
            result.totalMinutes += value.totalMinutes;
        });

        return result;
    }";


            var finalize = @"
    function(key, value){
      
      value.average = value.totalMinutes / value.count;
      return value;

    }";


            //var x = MongoDB.Bson.BsonJavaScript.Create()

            var client = new MongoClient("mongodb://test:test@ds041188.mongolab.com:41188/testreduce1");

            var database = client.GetDatabase("testreduce1");

            //var m = new MapReduceOptions<>

            var collection = database.GetCollection<Movie>("movies");
            
            //var builder = new MapReduceOutputToCollectionOperation()
            //var y = new MapReduceOperation<>
            
            var options = new MapReduceOptions<Movie, Dao>(); //new MapReduceOptionsBuilder();
            options.JavaScriptMode = true;
            options.OutputOptions.ToJson();

            //var col = new MapReduceOutputToCollectionOperation(new CollectionNamespace("testreduce1", "movies"),
            //    new CollectionNamespace("testreduce1", "reduce1"), map, reduce, new MessageEncoderSettings());


            //var x = col.ExecuteAsync()


            

            //options.OutputOptions.
            //options.
            //options.OutputOptions.ToJson(new JsonWriterSettings());
            //options.
            //options.SetFinalize(finalize);
            //options.SetOutput(MapReduceOutput.Inline);
            //var results = collection.MapReduceAsync(map, reduce, options).ToJson();



            //var h = results;
            //results.Start();



            //results.Wait();


            //var x = results.Result;


            //results.Wait();

            //results.



            //options
            //var results = collection.MapReduce(map, reduce, options);

            //foreach (var result in results.GetResults())
            //{
            //    Console.WriteLine(result.ToJson());
            //}

        }


        private static void Mr()
        {

                     var map = @"
    function() {
        var movie = this;
        emit(movie.Category, { count: 1, totalMinutes: movie.Minutes });
    }";

            var reduce = @"        
    function(key, values) {
        var result = {count: 0, totalMinutes: 0 };

        values.forEach(function(value){               
            result.count += value.count;
            result.totalMinutes += value.totalMinutes;
        });

        return result;
    }";


            var finalize = @"
    function(key, value){
      
      value.average = value.totalMinutes / value.count;
      return value;

    }";


            var client = new MongoClient("mongodb://test:test@ds041188.mongolab.com:41188/testreduce1");

            var database = client.GetDatabase("testreduce1");

            //var m = new MapReduceOptions<>

            var collection = database.GetCollection<Movie>("movies");
            var options = new MapReduceOptions<Movie, Dao>();  //new MapReduceOptionsBuilder();
            options.JavaScriptMode = true;
            options.OutputOptions.ToJson();
            //options.
            options.OutputOptions.ToJson(new JsonWriterSettings());
            //options.
            //options.SetFinalize(finalize);
            //options.SetOutput(MapReduceOutput.Inline);
            var results = collection.MapReduceAsync(map, reduce, options);

            //results.Start();





            results.Wait();


            var x = results.Result;


            //results.Wait();

            //results.



            //options
            //var results = collection.MapReduce(map, reduce, options);

            //foreach (var result in results.GetResults())
            //{
            //    Console.WriteLine(result.ToJson());
            //}

        }
    }

    public class Movie
    {
        public string Title { get; set; }
        public string Category { get; set; }
        public int Minutes { get; set; }

    }

    public class Dao
    {
        public string _id { get; set; }

        public string value { get; set; }

        public Object item { get; set; }
        //public Item[] item {  }

    }


    public class Item
    {
        public int count { get; set; }
        public int totalMinutes { get; set; }
        public double average { get; set; }
    }
}
