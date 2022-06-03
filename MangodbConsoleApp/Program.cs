using MongoDB.Bson;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace MangodbConsoleApp
{
    class Program
    {
        static void Main(string[] args)
        {
            Thread thread = new Thread(async () =>
            {
                MongoHelper<Students> studentsMongoHeaper = new MongoHelper<Students>("mongodb://127.0.0.1:27017", "test", "Students");
                //增加
                var insertMsg = await studentsMongoHeaper.InsertAsync(new Students() { Name = "dxw1", Age = 18, Sex = true });
                Console.WriteLine(insertMsg);
                //删除
                var msg = await studentsMongoHeaper.DeleteManyAsync("Name", "dxw", true);
                Console.WriteLine(msg);
                //查询
                var students = await studentsMongoHeaper.QueryManyAsync("Name", "dxw1");
                if (students != null && students.Count > 0)
                {
                    foreach (var stu in students)
                    {
                        Console.WriteLine(stu);
                    }
                }
            });
            thread.Start();

            Console.ReadKey();
        }
    }

    class Students
    {
        public ObjectId _id { get; set; }
        public string Name { get; set; }
        public int Age { get; set; }
        public bool Sex { get; set; }

        public override string ToString()
        {
            return $"id:{_id} Name:{Name} Age:{Age} Sex:{Sex}";
        }
    }
}
