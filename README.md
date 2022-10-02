#### flink hudi example
* flink DataGen connector生产数据写入到Hudi表，测试流程使用

```shell
# build
mvn clean package -Dscope.type=provided 
# 或者使用build好的jar
wget https://dxs9dnjebzm6y.cloudfront.net/tmp/emr-flink-hudi-1.0.jar
# emr flink-hudi-budle 拷贝到flink lib下
sudo cp /usr/lib/hudi/hudi-flink-bundle.jar /usr/lib/flink/lib/
# check-leaked-classloader set false
sudo sed -i -e '$a\classloader.check-leaked-classloader: false' /etc/flink/conf/flink-conf.yaml
# 运行作业 ，注意替换为自己的S3路径
sudo flink run -m yarn-cluster  -yjm 1024 -ytm 2048 -d -ys 4 -p 8 -c  com.aws.analytics.DataGen2Hudi /home/hadoop/emr-flink-hudi-1.0.jar -c s3://app-util/flink-data-gen/chk/ -p s3://app-util/flink-data-gen-hudi-02/ -t test_tb

# 作业参数说明如下
DataGen2Hudi 1.0
Usage: DataGen2Hudi [options]

  -c, --checkpointDir <value>
                           checkpoint dir
  -l, --checkpointInterval <value>
                           checkpoint interval: default 60 seconds
  -p, --hudiPath <value>   hudi path: eg. s3://xxx/xxx/
  -t, --hudiTableName <value>
                           hudi table name
  -r, --rowsPerSecond <value>
                           ddatagen rows-per-second, default:100
```

![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221002205221.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221002205245.png)
![](https://pcmyp.oss-accelerate.aliyuncs.com/markdown/20221002205325.png)