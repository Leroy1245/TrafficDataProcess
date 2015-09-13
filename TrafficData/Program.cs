using NServiceKit.Redis;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TrafficData
{
    class Program
    {
        public static string DataPath = "E:/TrafficData/2014-09-07.dat";
        public static string OutputPath = "E:/TrafficData/Collect.csv";
        public static string OutputRoadDir = "E:/TrafficData/按路段分/";
        public static string SplitRoadDir = "E:/TrafficData/按路段分(原数据)/";



        static void Main(string[] args)
        {
            SplitDataByRoad();
            //ReadData();
            //OutPutData();
            //OutPutDataByRoad();
        }

        /// <summary>
        /// 汇总输出
        /// </summary>
        public static void OutPutData()
        {
            string host = "localhost";


            using (RedisClient redisClient = new RedisClient(host))
            {
                var locateCount = redisClient.ZCard("AllLocates");
                var allLocateData = redisClient.ZRange("AllLocates", 0, (int)locateCount);
                List<string> allLocate = new List<string>();
                for (int i = 0; i < allLocateData.Length; i++)
                {
                    allLocate.Add(System.Text.Encoding.UTF8.GetString(allLocateData[i]));
                }
                allLocate = allLocate.OrderBy(p => p).ToList();

                Console.WriteLine("开始输出时间序列数据");
                FileStream fs = new FileStream(OutputPath, FileMode.Create);
                StreamWriter sw = new StreamWriter(fs, Encoding.UTF8);
                long total = 0;
                for (int h = 0; h < 24; h++)
                {
                    for (int m = 0; m < 60; m += 5)
                    {
                        var pre = string.Format("2014-09-07-{0:D2}:{1:D2}", h, m);
                        Console.WriteLine(string.Format("------------时段：{0}------------", pre));
                        for (int i = 0; i < allLocate.Count; i++)
                        {
                            string key = pre + "@" + allLocate[i];
                            var value = redisClient.Get(key);
                            string count = (value == null ? "0" : System.Text.Encoding.Default.GetString(value));
                            total += int.Parse(count);
                            sw.WriteLine(string.Format("{0},{1},{2}", pre, allLocate[i], count));
                        }
                    }

                }
                sw.Close();
                fs.Close();
                Console.WriteLine(string.Format("------------总和：{0}------------", total));
            }
        }

        /// <summary>
        /// 分路段输出数据
        /// </summary>
        public static void OutPutDataByRoad()
        {
            string host = "localhost";


            using (RedisClient redisClient = new RedisClient(host))
            {
                var locateCount = redisClient.ZCard("AllLocates");
                var allLocateData = redisClient.ZRange("AllLocates", 0, (int)locateCount);
                List<string> allLocate = new List<string>();
                for (int i = 0; i < allLocateData.Length; i++)
                {
                    allLocate.Add(System.Text.Encoding.UTF8.GetString(allLocateData[i]));
                }
                allLocate = allLocate.OrderBy(p => p).ToList();

                Console.WriteLine("开始输出时间序列数据");
                for (int i = 0; i < allLocate.Count; i++)
                {
                    Console.WriteLine(string.Format("------------路段：{0}------------", allLocate[i]));
                    FileStream fs = new FileStream(OutputRoadDir + allLocate[i] + ".csv", FileMode.Create);
                    StreamWriter sw = new StreamWriter(fs, Encoding.UTF8);
                    for (int h = 0; h < 24; h++)
                    {
                        for (int m = 0; m < 60; m += 5)
                        {
                            var pre = string.Format("2014-09-07-{0:D2}:{1:D2}", h, m);
                            string key = pre + "@" + allLocate[i];
                            var value = redisClient.Get(key);
                            string count = (value == null ? "0" : System.Text.Encoding.Default.GetString(value));
                            sw.WriteLine(string.Format("{0},{1},{2}", pre, allLocate[i], count));
                        }
                    }
                    sw.Close();
                    fs.Close();
                }
            }
        }

        /// <summary>
        /// 分时段输出数据
        /// </summary>
        public static void OutPutDataByTime()
        {
            string host = "localhost";


            using (RedisClient redisClient = new RedisClient(host))
            {
                var locateCount = redisClient.ZCard("AllLocates");
                var allLocateData = redisClient.ZRange("AllLocates", 0, (int)locateCount);
                List<string> allLocate = new List<string>();
                for (int i = 0; i < allLocateData.Length; i++)
                {
                    allLocate.Add(System.Text.Encoding.UTF8.GetString(allLocateData[i]));
                }
                allLocate = allLocate.OrderBy(p => p).ToList();

                Console.WriteLine("开始输出时间序列数据");


                for (int h = 0; h < 24; h++)
                {
                    for (int m = 0; m < 60; m += 5)
                    {
                        var pre = string.Format("2014-09-07-{0:D2}:{1:D2}", h, m);
                        Console.WriteLine(string.Format("------------时段：{0}------------", pre));
                        FileStream fs = new FileStream("E:/TrafficData/按时段分/" + pre + ".csv", FileMode.Create);
                        StreamWriter sw = new StreamWriter(fs, Encoding.UTF8);
                        for (int i = 0; i < allLocate.Count; i++)
                        {
                            string key = pre + "@" + allLocate[i];
                            var value = redisClient.Get(key);
                            string count = (value == null ? "0" : System.Text.Encoding.Default.GetString(value));
                            sw.WriteLine(string.Format("{0},{1},{2}", pre, allLocate[i], count));
                        }
                        sw.Close();
                        fs.Close();
                    }

                }
            }
        }

        public static void ReadData()
        {
            string host = "localhost";

            using (RedisClient redisClient = new RedisClient(host))
            {
                StreamReader sr = new StreamReader(DataPath);
                Console.WriteLine("开始读取原数据");
                var locDic = new Dictionary<string, int>();
                var sw = new Stopwatch();
                sw.Start();
                string s = sr.ReadLine();
                long i = 0;
                while (s != null)
                {
                    if (i % 10000 == 0)
                        Console.WriteLine("已读取数据量:" + i);
                    var sData = s.Split('\t');
                    var dateSplit = sData[4].Split(':');
                    int min = int.Parse(dateSplit[1]);
                    min = (min / 5) * 5;
                    var locate = sData[6];
                    string key = string.Format("{0}:{1:D2}@{2}", dateSplit[0], min, locate);
                    redisClient.Incr(key);
                    locDic[locate] = 0;
                    s = sr.ReadLine();
                    i++;
                }
                locDic.Keys.ToList().ForEach(p =>
                {
                    redisClient.ZAdd("AllLocates", 0, System.Text.Encoding.UTF8.GetBytes(p));
                });
                sw.Stop();
                Console.WriteLine("读取完毕，总数据量：" + i + "，时间：" + sw.ElapsedMilliseconds + " ms");
            }

        }

        public static void SplitDataByRoad()
        {
            StreamReader sr = new StreamReader(DataPath);
            Console.WriteLine("开始读取原数据");
            var locDic = new Dictionary<string, StreamWriter>();
            var sw = new Stopwatch();
            sw.Start();
            string s = sr.ReadLine();
            long i = 0;
            while (s != null)
            {
                if (i % 10000 == 0)
                    Console.WriteLine("已读取数据量:" + i);
                var sData = s.Split('\t');
                var date = sData[4];
                var locate = sData[6];
                var carPlate = sData[3];
                string data = string.Format("{0},{1},{2}",date,locate,carPlate);
                
                if(locDic.ContainsKey(locate))
                {
                    locDic[locate].WriteLine(data);
                }
                else
                {
                    locDic[locate] = new StreamWriter(SplitRoadDir+locate+".csv");
                    locDic[locate].WriteLine(data);
                }
                s = sr.ReadLine();
                i++;
            }

            foreach (var item in locDic)
            {
                item.Value.Close();
            }
            sw.Stop();
            Console.WriteLine("读取完毕，总数据量：" + i + "，时间：" + sw.ElapsedMilliseconds + " ms");

        }
    }
}
