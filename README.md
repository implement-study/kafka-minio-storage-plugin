# kafka-minio-storage-plugin

这是一个开箱即用的Kafka分层存储的demo

## 使用 

### Minio Server

如果使用Docker可以直接运行命令
```shell
docker run -d  \
  --name minio \
  -p 9000:9000 \
  -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
   -v `pwd`/data:/data \
  quay.io/minio/minio:RELEASE.2024-03-30T09-41-56Z server /data --console-address ":9001"

```

### 打包命令

你可以直接运行gradlew task, 运行之后你会在控制台看到一个配置，直接把配置粘贴到kafka broker properties中，然后启动broker

```shell
./gradlew buildMinioPlugin

```


### 创建主题

你可以直接运行下面的gradle task , 创建一个分层存储的主题 
```shell

./gradlew createTieredTopic -PtopicName=test1 -Pbootstrap=localhost:9092

```

### 生产数据

你可以直接运行下面的gradle task, 给目标topic 生产顺序数据，多次执行也是有序的，可以指定执行间隔和每次发送的数据量

```shell
./gradlew produceData -PtopicName=test1 -Pbootstrap=localhost:9092 -PdelayMillis=5000 -Pcount=300
```
